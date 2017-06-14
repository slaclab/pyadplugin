#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time  # NOQA
import logging
from pyadplugin import ADPluginServer, ADPluginPV

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def wfsum(array, height=None, width=None):
    # time.sleep(1)  # To check how it works with slow plugins
    # logger.debug('we did a callback')
    return sum(array)


# Switch this to a camera you'd like to test with
ad_prefix = 'HX2:SB1:CVV:01:'

server = ADPluginServer(prefix='SUM:',
                        ad_prefix=ad_prefix,
                        stream='IMAGE1',
                        min_cbtime=10,
                        enable_callbacks=True)
pv = ADPluginPV("SUM", 0, wfsum, server)
