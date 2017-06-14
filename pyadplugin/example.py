#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time  # NOQA
import logging
from pyadplugin import ADPluginServer, ADPluginPV

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def wfsum(array):
    # time.sleep(1)
    # logger.debug('we did a callback')
    return sum(array)


server = ADPluginServer(prefix='SUM:',
                        ad_prefix='HX2:SB1:CVV:01:',
                        stream='IMAGE1',
                        min_cbtime=10,
                        enable_callbacks=True)
pv = ADPluginPV("SUM", 0, wfsum, server)
