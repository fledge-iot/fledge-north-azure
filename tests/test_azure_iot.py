# -*- coding: utf-8 -*-

# FLEDGE_BEGIN
# See: http://fledge-iot.readthedocs.io/
# FLEDGE_END

from unittest.mock import patch
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
        'name': 'Azure IoT Hub device client',
        'version': '2.3.0',
        'type': 'north',
        'interface': '1.0',
        'config': config
    }


def test_plugin_init():
    with patch.object(azure_iot, 'AzureIoTHubDeviceClient'):
        actual = azure_iot.plugin_init(config)
        del actual['azure_iot_hub_device_client']
        del actual['max_retry_count']
        assert actual == config


@pytest.mark.skip(reason="To be implemented")
async def test_plugin_send():
    pass


def test_plugin_reconfigure():
    assert azure_iot.plugin_reconfigure


def test_plugin_shutdown():
    handle = azure_iot._DEFAULT_CONFIG
    handle['azure_iot_hub_device_client'] = azure_iot.AzureIoTHubDeviceClient
    with patch.object(azure_iot.AzureIoTHubDeviceClient, 'shutdown') as patch_shutdown:
        assert azure_iot.plugin_shutdown(handle) is None
        assert handle['azure_iot_hub_device_client'] is None
    patch_shutdown.assert_called_once_with()
