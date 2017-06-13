#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging
from pyadplugin import ADPluginServer, ADPluginPV

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def wfsum(array):
    time.sleep(1)
    return sum(array)


server = ADPluginServer(prefix="HX2:SB1:CVV:01:SUM:",
                        source="HX2:SB1:CVV:01:IMAGE2:ArrayData",
                        min_cbtime=10, enable_callbacks=1)
pv = ADPluginPV("SUM", 0, wfsum, server)
