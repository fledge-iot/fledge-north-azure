# -*- coding: utf-8 -*-

""" Azure North plugin"""
import asyncio
import json
import sys
import logging

from fledge.common import logger

# Using the Python Azure Device SDK for IoT Hub:
# https://github.com/Azure/azure-iot-sdk-python
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message, MethodResponse

__author__ = "Sebastian Kropatschek"
__copyright__ = "Copyright (c) 2020 Kapsch & Austrian Center for Digital Production (ACDP)"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)

_DEFAULT_CONFIG = {
    'plugin': {
         'description': 'Azure North Plugin',
         'type': 'string',
         'default': 'azure_iot',
         'readonly': 'true'
    },
    'primaryConnectionString': {
        'description': 'Connection string based on primary key used in API calls which allows device to '
                       'communicate with Azure IoT Hub',
        'type': 'string',
        'default': 'HostName=<Host Name>;DeviceId=<Device Name>;SharedAccessKey=<Device Key>',
        'order': '1',
        'displayName': 'Primary Connection String'
    },
    "websockets": {
        "description": "Set to true if using MQTT over websockets",
        "type": "boolean",
        "default": "false",
        'order': '2',
        'displayName': 'MQTT over websockets'
    },
    "source": {
         "description": "Source of data to be sent on the stream. May be either readings or statistics.",
         "type": "enumeration",
         "default": "readings",
         "options": ["readings", "statistics"],
         'order': '3',
         'displayName': 'Source'
    }
}


def plugin_info():
    return {
        'name': 'Azure IoT',
        'version': '2.1.1',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(data):
    global azure_north, config
    azure_north = AzureNorthPlugin()
    config = data
    return config


async def plugin_send(data, payload, stream_id):
    try:
        _LOGGER.info(f'Azure North Python - plugin_send: {stream_id}')
        is_data_sent, new_last_object_id, num_sent = await azure_north.send_payloads(payload)
    except asyncio.CancelledError as ex:
        _LOGGER.exception(f'Exception occurred in plugin_send: {ex}')
    else:
        return is_data_sent, new_last_object_id, num_sent


def plugin_shutdown(data):
    pass


def plugin_reconfigure():
    pass


class AzureNorthPlugin(object):
    """ North Azure Plugin """

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()

    async def send_payloads(self, payloads):
        is_data_sent = False
        last_object_id = 0
        num_sent = 0

        MESSAGE_SIZE_LIMIT = 262144  # Limit form the Azure IoT Hub

        size_payload_block = 0

        try:
            payload_block = list()

            for p in payloads:
                read = dict()
                read["asset"] = p['asset_code']
                read["readings"] = p['reading']
                read["timestamp"] = p['user_ts']
                
                size_payload_block += sys.getsizeof(json.dumps(read, separators=(',', ':')).encode('utf-8'))
                if size_payload_block > MESSAGE_SIZE_LIMIT * 0.9: 
                    # less than 90% of the maximum value is a quick solution to catch the not yet calculated
                    # overhead of the message class. Will be improved in a future version
                    _LOGGER.warning("The size of the message is larger than 256 kB! "
                                    "The remaining payloads will be sent on the next function call")
                    break
                
                last_object_id = p["id"]
                payload_block.append(read)
                
            num_sent = await self._send_payloads(payload_block)
            is_data_sent = True
        except Exception as ex:
            _LOGGER.exception("Data could not be sent, %s", str(ex))

        return is_data_sent, last_object_id, num_sent

    async def _send_payloads(self, payload_block):
        """ send a list of block payloads"""
        
        num_count = 0
        try:
            device_client = IoTHubDeviceClient.create_from_connection_string(
                config["primaryConnectionString"]["value"], websockets=config["websockets"]["value"])

            # Connect the device client.
            await device_client.connect()
            
            await self._send(device_client, payload_block)
            
            # finally, disconnect
            await device_client.disconnect()

        except Exception as ex:
            _LOGGER.exception(f'Exception sending payloads: {ex}')
        else:
            num_count += len(payload_block)
        return num_count

    async def _send(self, client, payload):
        """ Send the payload, using provided client """
       
        message = Message(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
        message.content_encoding = "utf-8"
        message.content_type = "application/json"
        size = str(message.get_size())
        _LOGGER.debug("Sending message: {} and its size: {}".format(message, size))
        await client.send_message(message)
