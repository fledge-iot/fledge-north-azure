# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

import pytest
from python.fledge.plugins.north.azure_iot import azure_iot

__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2023 Dianomic Systems Inc."
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

config = azure_iot._DEFAULT_CONFIG


def test_plugin_contract():
    assert callable(getattr(azure_iot, 'plugin_info'))
    assert callable(getattr(azure_iot, 'plugin_init'))
    assert callable(getattr(azure_iot, 'plugin_send'))
    assert callable(getattr(azure_iot, 'plugin_reconfigure'))
    assert callable(getattr(azure_iot, 'plugin_shutdown'))


def test_plugin_info():
    assert azure_iot.plugin_info() == {
        'name': 'Azure IoT',
        'version': '2.1.0',
        'type': 'north',
        'interface': '1.0',
        'config': config
    }


def test_plugin_init():
    actual = azure_iot.plugin_init(config)
    del actual['azure_north']
    assert actual == config


@pytest.mark.skip(reason="To be implemented")
async def test_plugin_send():
    pass


def test_plugin_reconfigure():
    assert azure_iot.plugin_reconfigure


def test_plugin_shutdown():
    assert azure_iot.plugin_shutdown(azure_iot._DEFAULT_CONFIG) is None
