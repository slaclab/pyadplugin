#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep
from threading import Thread, RLock
from queue import Queue
import logging

from epics import PV
from epics.ca import CAThread
from pypvserver import PypvServer, PyPV

logger = logging.getLogger(__name__)


class ADPluginServer:
    def __init__(self, prefix, source, queuesize=5, enable_callbacks=0,
                 min_cbtime=0):
        self.server = PypvServer(prefix)
        self.ad_directory = {}
        self.settings_lock = RLock()
        self.plugins = {}
        self.has_update = False
        self.enable_callbacks = int(enable_callbacks)
        self.min_cbtime = float(min_cbtime)
        self.queue = None
        queuesize = int(queuesize)

        self._ndarray_port_cb(value=str(source))
        self._add_builtin('NDArrayPort', str(source), cb=self._ndarray_port_cb)
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

        def callback(value=None, **kwargs):
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

    def _initialize_pv(self, pvname):
        self.mon_pv = PV(pvname, callback=self._mark_has_update)
        self.mon_pv._args['count'] = 1
        self.mon_pv.auto_monitor = True
        self.get_pv = PV(pvname)
        self.mon_pv.connect(timeout=0)
        self.get_pv.connect(timeout=0)

    def _mark_has_update(self, **kwargs):
        self.has_update = True

    def _get_queue_loop(self, **kwargs):
        while True:
            logger.debug('start get queue')
            with self.settings_lock:
                if all((self.enable_callbacks, self.plugins,
                        self.has_update)):
                    logger.debug('we will try to get an array')
                    self.has_update = False
                    if self.queue.full():
                        logger.debug('queue was full')
                        dropped_pv = self.ad_directory['DroppedArrays']
                        dropped_pv.put(dropped_pv.get() + 1)
                        logger.debug('dropped %s arrays total',
                                     dropped_pv.get())
                    else:
                        array = self.get_pv.get()
                        logger.debug('we got an array, stashing into queue')
                        self.queue.put(array)
                    did_cb = True
                else:
                    logger.debug('we will not try to get an array,'
                                 + 'enable_callbacks=%s, num_plugins=%s,'
                                 + 'has_update=%s', self.enable_callbacks,
                                 len(self.plugins), self.has_update)
                    did_cb = False
                queue_use_pv = self.ad_directory['QueueUse']
                logger.debug('queue has %s elements', self.queue.qsize())
                queue_use_pv.put(self.queue.qsize())

            logger.debug('post get queue sleep')
            if did_cb:
                sleep(self.min_cbtime)
            else:
                sleep(max(self.min_cbtime, 1))

    def _array_cb_loop(self):
        while True:
            logger.debug('start array cb loop')
            array = None
            with self.settings_lock:
                if self.enable_callbacks and self.plugins:
                    logger.debug('lets try to get a queued array')
                    did_cb = True
                    if not self.queue.empty():
                        array = self.queue.get()
                        logger.debug('got an array!')
                else:
                    did_cb = False

            if array is not None:
                logger.debug('run the plugins now')
                for plugin in self.plugins.values():
                    plugin(array)

            logger.debug('post array cb sleep')
            if did_cb:
                sleep(self.min_cbtime)
            else:
                sleep(max(self.min_cbtime, 1))


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
