# -*- coding: utf-8 -*-

""" Azure IoT Hub North plugin"""
# Using the Python Device SDK for IoT Hub:
#   https://github.com/Azure/azure-iot-sdk-python
from azure.iot.device import IoTHubDeviceClient, Message, MethodResponse


import time
import asyncio
import json

from fledge.common import logger
from fledge.plugins.north.common.common import *

__author__ = "Sebastian Kropatschek"
__copyright__ = "Copyright (c) 2020 Kapsch & ACDP"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)

_CONFIG_CATEGORY_NAME = "AZURE"
_CONFIG_CATEGORY_DESCRIPTION = "Azure IoT Hub Python North Plugin"

_DEFAULT_CONFIG = {
    'plugin': {
         'description': 'Azure IoT Hub North Plugin',
         'type': 'string',
         'default': 'azure',
         'readonly': 'true'
    },
    'primary_connection_string': {
        'description': 'Connection string based on primary key used in API calls which allows device to communicate with Azure IoT Hub',
        'type': 'string',
        'default': 'HostName=<Host Name>;DeviceId=<Device Name>;SharedAccessKey=<Device Key>',
        'order': '1',
        'displayName': 'Primary Connection String'
    },
    'azure_iot_topic': {
        'description': 'Azure IoT Topic',
        'type': 'string',
        'default': 'iot-readings',
        'order': '2',
        'displayName': 'Azure IoT Topic'
    },
    "source": {
         "description": "Source of data to be sent on the stream. May be either readings or statistics.",
         "type": "enumeration",
         "default": "readings",
         "options": [ "readings", "statistics" ],
         'order': '3',
         'displayName': 'Source'
    },
    "applyFilter": {
        "description": "Should filter be applied before processing data",
        "type": "boolean",
        "default": "false",
        'order': '4',
        'displayName': 'Apply Filter'
    },
    "filterRule": {
        "description": "JQ formatted filter to apply (only applicable if applyFilter is True)",
        "type": "string",
        "default": ".[]",
        'order': '5',
        'displayName': 'Filter Rule'
    }
}

def plugin_info():
    return {
        'name': 'azure_iot_north_python',
        'version': '0.0.1',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }

def plugin_init(data):
    _LOGGER.info('Initializing Azure IoT North Python Plugin')
    global azure_iot_north, config
    azure_iot_north = AzureNorthPlugin()
    config = data
    _LOGGER.info(f'Initializing plugin with Primary Connection String: {config["primary_connection_string"]["value"]}')
    return config

async def plugin_send(data, payload, stream_id):
    try:
        _LOGGER.info(f'Azure IoT North Python - plugin_send: {stream_id}')
        is_data_sent, new_last_object_id, num_sent = await azure_iot_north.send_payloads(payload)
    except asyncio.CancelledError as ex:
        _LOGGER.exception(f'Exception occurred in plugin_send: {ex}')
    else:
        _LOGGER.info('payload sent successfully')
        return is_data_sent, new_last_object_id, num_sent

def plugin_shutdown(data):
    pass

# TODO: North plugin can not be reconfigured? (per callback mechanism)
def plugin_reconfigure():
    pass

class AzureNorthPlugin(object):
    """ North Azure IoT Plugin """

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()

    def azure_iot_error(self, error):
        _LOGGER.error(f'Azure IoT error: {error}')

    async def send_payloads(self, payloads):
        is_data_sent = False
        last_object_id = 0
        num_sent = 0

        try:
            _LOGGER.info('processing payloads')
            payload_block = list()

            for p in payloads:
                last_object_id = p["id"]
                read = dict()
                read["asset"] = p['asset_code']
                read["readings"] = p['reading']
                read["timestamp"] = p['user_ts']
                payload_block.append(read)

            num_sent = await self._send_payloads(payload_block)
            _LOGGER.info('payloads sent: {num_sent}')
            is_data_sent = True
        except Exception as ex:
            _LOGGER.exception("Data could not be sent, %s", str(ex))

        return is_data_sent, last_object_id, num_sent

    async def _send_payloads(self, payload_block):
        """ send a list of block payloads"""
        num_count = 0
        try:
            # TODO ADD config parameter for websockets
            #device_client = IoTHubDeviceClient.create_from_connection_string(config["primary_connection_string"]["value"], websockets=True)
            
            device_client = IoTHubDeviceClient.create_from_connection_string(config["primary_connection_string"]["value"])

            _LOGGER.info(f'Using Primary Connection String: {config["primary_connection_string"]["value"]}')
            
            # Connect the device client.
            #await device_client.connect()
            
            await self._send(device_client, payload_block)
            
            # finally, disconnect
            #await device_client.disconnect()
        except Exception as ex:
            _LOGGER.exception(f'Exception sending payloads: {ex}')
        else:
            num_count += len(payload_block)
        return num_count

    async def _send(self, client, payload):
        """ Send the payload, using provided client """
        message = Message(json.dumps(payload).encode('utf-8'))
        _LOGGER.info("Sending message: {}".format(message))
        client.send_message(message)
        _LOGGER.info('Message successfully sent')

