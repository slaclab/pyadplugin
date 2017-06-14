#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep, time
from threading import Thread, RLock, Event
from queue import Queue, Empty
import logging

from epics import PV
from epics.ca import CAThread
from pypvserver import PypvServer, PyPV

logger = logging.getLogger(__name__)


class ADPluginServer:
    """
    Server to mimic some of the plugin records/functionality from AreaDetector.
    The goal was for the user to only need to supply configuration parameters
    and functions to run that take the image np.ndarray as an argument. These
    servers have the following PVs: (set them from the equivalent non-_RBV PV)

    Attributes
    ----------
    $(ad_prefix)$(prefix)NDArrayPort_RBV:
        In AD this is a port name, but here we have to take from an array pv.
        This is where we store the stream argument e.g. IMAGE1

    $(ad_prefix)$(prefix)EnableCallbacks:
        If this is set to 0, The plugin will be disabled. Otherwise, the plugin
        will be active.
    """
    def __init__(self, prefix, ad_prefix, stream,
                 enable_callbacks=0, min_cbtime=0, queuesize=5):
        self.server = PypvServer(ad_prefix + prefix)
        self.ad_prefix = ad_prefix
        self.ad_directory = {}
        self.settings_lock = RLock()
        self.plugins = {}
        self.has_update = Event()
        self.enable_callbacks = int(enable_callbacks)
        self.min_cbtime = float(min_cbtime)
        self.queue = None
        queuesize = int(queuesize)

        self._ndarray_port_cb(value=str(stream))
        self._add_builtin('NDArrayPort', str(stream), cb=self._ndarray_port_cb)
        self._enable_callbacks_cb(value=self.enable_callbacks)
        self._add_builtin('EnableCallbacks', self.enable_callbacks,
                          cb=self._enable_callbacks_cb)
        self._min_cbtime_cb(value=self.min_cbtime)
        self._add_builtin('MinCallbackTime', self.min_cbtime,
                          cb=self._min_cbtime_cb)
        self._queuesize_cb(value=queuesize)
        self._add_builtin('QueueSize', queuesize, cb=self._queuesize_cb)
        self._add_builtin('QueueUse', 0)
        self._add_builtin('DroppedArrays', 0)

        arrays = CAThread(target=self._array_cb_loop, args=(), daemon=True)
        plugins = Thread(target=self._get_queue_loop, args=(), daemon=True)
        arrays.start()
        plugins.start()

    def _add_builtin(self, name, value, cb=None):
        rbv = PyPV(name + '_RBV', value=value, server=self.server)
        if cb is None:
            def cb(**kwargs):
                pass

        def callback(value=None, rbv=rbv, cb=cb, **kwargs):
            rbv.put(value)
            cb(value=value, **kwargs)
        setter = PyPV(name, value=value, server=self.server,
                      written_cb=callback)
        self.ad_directory[rbv.name] = rbv
        self.ad_directory[setter.name] = setter

    def install_plugin(self, plugin):
        with self.settings_lock:
            self.plugins[plugin.name] = plugin

    def uninstall_plugin(self, plugin):
        with self.settings_lock:
            del self.plugins[plugin.name]

    def _ndarray_port_cb(self, *, value, **kwargs):
        with self.settings_lock:
            self._initialize_pv(value)

    def _enable_callbacks_cb(self, *, value, **kwargs):
        with self.settings_lock:
            if value:
                self.enable_callbacks = True
            else:
                self.enable_callbacks = False

    def _min_cbtime_cb(self, *, value, **kwargs):
        with self.settings_lock:
            if value < 0:
                value = 0
            self.min_cbtime = value

    def _queuesize_cb(self, *, value, **kwargs):
        with self.settings_lock:
            if value < 1:
                value = 1
            if self.queue is None:
                self.queue = Queue(value)
            else:
                new_queue = Queue(value)
                while not self.queue.empty() and not new_queue.full():
                    new_queue.put(self.queue.get(block=False))
                self.queue = new_queue

    def _initialize_pv(self, stream):
        get_pvname = self.ad_prefix + stream + ':ArrayData'
        logger.debug('getting data from %s', get_pvname)
        self.get_pv = PV(get_pvname)
        self.get_pv.connect(timeout=0)

        mon_pvname = self.ad_prefix + stream + ':UniqueId_RBV'
        logger.debug('monitoring changes from %s', mon_pvname)
        self.mon_pv = PV(mon_pvname, callback=self._mark_has_update,
                         auto_monitor=True)

    def _mark_has_update(self, **kwargs):
        self.has_update.set()

    def _update_queue_use(self):
        with self.settings_lock:
            queue_use_pv = self.ad_directory['QueueUse_RBV']
            logger.debug('queue has %s elements', self.queue.qsize())
            queue_use_pv.put(self.queue.qsize())

    def _get_queue_loop(self, **kwargs):
        while True:
            start = time()
            logger.debug('start get queue')
            with self.settings_lock:
                queue = self.queue
                if self.enable_callbacks and self.plugins:
                    logger.debug('we will try to get an array from epics')
                    get_array = True
                else:
                    logger.debug('we will not try to get an array from epics,'
                                 + 'enable_callbacks=%s, num_plugins=%s',
                                 self.enable_callbacks, len(self.plugins))
                    get_array = False

            success = False
            if get_array:
                ok = self.has_update.wait(timeout=1.0)
                if ok:
                    if queue.full():
                        logger.debug('queue was full')
                        dropped_pv = self.ad_directory['DroppedArrays_RBV']
                        n_dropped = dropped_pv.get()
                        dropped_pv.put(n_dropped + 1)
                        logger.debug('dropped %s arrays total', n_dropped)
                    else:
                        array = self.get_pv.get()
                        logger.debug('we got an array, stashing into queue')
                        queue.put(array)
                        self._update_queue_use()
                    # Mark success True even if the Queue was full so we hit
                    # the minimum callback sleep and we don't barrage the
                    # python process's cpu usage with dropped array counts
                    success = True
                else:
                    logger.debug('wait for image update timed out after 1s')
            else:
                sleep(1)

            if success:
                elapsed = time() - start
                sleep(max(self.min_cbtime - elapsed, 0))

    def _array_cb_loop(self):
        while True:
            start = time()
            logger.debug('start array cb loop')
            array = None
            with self.settings_lock:
                queue = self.queue
                if self.enable_callbacks and self.plugins:
                    logger.debug('lets wait for a queued array...')
                    get_array = True
                else:
                    logger.debug('we will not get a queued array,'
                                 + 'enable_callbacks=%s, num_plugins=%s',
                                 self.enable_callbacks, len(self.plugins))
                    get_array = False

            if get_array:
                try:
                    array = queue.get(timeout=1)
                    logger.debug('got an array of type %s!', type(array))
                    if array is None:
                        logger.warning('No array: Make sure image PV exists!')
                    self._update_queue_use()
                except Empty:
                    pass

            if array is not None:
                logger.debug('run the plugins now')
                for plugin in self.plugins.values():
                    plugin(array)
                elapsed = time() - start
                logger.debug('post array cb sleep')
                sleep(max(self.min_cbtime - elapsed, 0))

            if not get_array:
                sleep(1)


class ADPluginPV(PyPV):
    def __init__(self, name, value, plugin, server, **kwargs):
        super().__init__(name, value, server=server.server, **kwargs)
        self.plugin = plugin
        self.adserver = server
        self.adserver.install_plugin(self)

    def __call__(self, array):
        logger.debug('run plugin %s', self.name)
        output = self.plugin(array)
        self.put(output)
        logger.debug('plugin %s success', self.name)
