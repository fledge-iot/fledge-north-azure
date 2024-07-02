# -*- coding: utf-8 -*-

"""Microsoft Azure IoT Hub device client North plugin"""
import asyncio
import json
import sys
import logging
from copy import deepcopy

from fledge.common import logger

# Using the Python Azure Device SDK for IoT Hub:
# https://github.com/Azure/azure-iot-sdk-python
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message

__author__ = "Sebastian Kropatschek"
__copyright__ = "Copyright (c) 2020 Kapsch & Austrian Center for Digital Production (ACDP)"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)

_DEFAULT_CONFIG = {
    "plugin": {
        "description": "Azure IoT Hub device client North Plugin",
        "type": "string",
        "default": "azure_iot",
        "readonly": "true"
    },
    "primaryConnectionString": {
        "description": "Connection string based on primary key used in API calls which allows "
                       "device to communicate with Azure IoT Hub",
        "type": "string",
        "default": "HostName=<Host Name>;DeviceId=<Device Name>;SharedAccessKey=<Device Key>",
        "order": "1",
        "displayName": "Primary Connection String",
        "mandatory": "true"
    },
    "websockets": {
        "description": "Set to true if using MQTT over websockets",
        "type": "boolean",
        "default": "false",
        "order": "2",
        "displayName": "MQTT over websockets"
    },
    "source": {
        "description": "Source of data to be sent on the stream. May be either readings or statistics.",
        "type": "enumeration",
        "default": "readings",
        "options": ["readings", "statistics"],
        "order": "3",
        "displayName": "Source"
    }
}


def plugin_info():
    """Used only once when call will be made to a plugin
    Args:

    Returns:
        Information about the plugin including the configuration for the plugin
    """
    return {
        'name': 'Azure IoT Hub device client',
        'version': '2.5.0',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(data):
    """Used for initialization of a plugin
    Args:
        data: Plugin configuration
    Returns:
        Dictionary of a Plugin configuration
    """
    config_data = deepcopy(data)
    config_data['azure_iot_hub_device_client'] = AzureIoTHubDeviceClient(config_data)
    config_data['max_retry_count'] = 10
    return config_data


async def plugin_send(handle, payload, stream_id):
    """Used to send the readings block to the configured destination
    Args:
        handle: An object which is returned by plugin_init
        payload: A list of readings block
        stream_id: An integer that uniquely identifies the connection from Fledge instance to the destination system
    Returns:
          Tuple which consists of
          - A Boolean that indicates if any data has been sent
          - The object id of the last reading which has been sent
          - Total number of readings which has been sent to the configured destination
    """
    try:
        azure_client = handle['azure_iot_hub_device_client']
        is_data_sent = False
        new_last_object_id = 0
        num_sent = 0
        if azure_client.client is None and handle['max_retry_count'] <= 10:
            handle['max_retry_count'] += 1
            await azure_client.connect()
        if azure_client.client is not None:
            is_data_sent, new_last_object_id, num_sent = await azure_client.send(payload)
    except asyncio.CancelledError:
        pass
    except ValueError as err:
        _LOGGER.error(err, "Bad Primary connection string to communicate with Azure IoT Hub.")
    else:
        return is_data_sent, new_last_object_id, num_sent


def plugin_shutdown(handle):
    """Used when plugin is no longer required and will be final call to the plugin
    Args:
        handle: An object which is returned by plugin_init
    Returns:
        None
    """
    azure_client = handle['azure_iot_hub_device_client']
    azure_client.shutdown()
    handle['azure_iot_hub_device_client'] = None


def plugin_reconfigure():
    pass


class AzureIoTHubDeviceClient(object):
    """A custom AzureIoTHubDeviceClient class"""

    def __init__(self, config):
        """"Initializer for AzureIoTHubDeviceClient
        Args:
            config: configuration data of plugin
        """
        self.event_loop = asyncio.get_event_loop()
        self.primary_connection_string = config["primaryConnectionString"]["value"]
        self.mqtt_over_websocket = True if config["websockets"]["value"].lower() == 'true' else False
        self.client = None

    async def send(self, payloads):
        """Sends a message to the default events endpoint on the Azure IoT Hub instance.
        Args:
            payloads: A list of readings block
        Returns:
            Tuple which consists of
            - A Boolean that indicates if any data has been sent
            - The object id of the last reading which has been sent
            - Total number of readings which has been sent to the configured destination
        """
        async def _send_message(pb):
            num_count = 0
            if self.client.connected:
                # separators used only to get minimum payload size to remove whitespaces around ',' ':'
                payload = json.dumps(pb, separators=(',', ':')).encode('utf-8')
                message = Message(data=payload, content_encoding="utf-8", content_type="application/json")
                _LOGGER.debug("Sending message: {} and of size: {}".format(message, str(message.get_size())))
                await self.client.send_message(message)
                num_count += len(pb)
            return num_count

        is_data_sent = False
        last_object_id = 0
        num_sent = 0
        message_size_limit = 262144  # Limit form the Azure IoT Hub
        size_payload_block = 0
        try:
            payload_block = []
            for p in payloads:
                read = {"asset": p['asset_code'], "readings": p['reading'], "timestamp": p['user_ts']}
                size_payload_block += sys.getsizeof(json.dumps(read, separators=(',', ':')).encode('utf-8'))
                if size_payload_block > message_size_limit * 0.9:
                    # less than 90% of the maximum value is a quick solution to catch the not yet calculated
                    # overhead of the message class. Will be improved in a future version
                    _LOGGER.warning("The size of the message is larger than 256 kB! "
                                    "The remaining payloads will be sent on the next function call")
                    break
                last_object_id = p["id"]
                payload_block.append(read)
            num_sent = await _send_message(payload_block)
            is_data_sent = True
        except Exception as ex:
            _LOGGER.exception(ex, 'Failed on sending payload.')
        return is_data_sent, last_object_id, num_sent

    async def connect(self):
        """Connects the client to an Azure IoT Hub instance."""
        self.client = IoTHubDeviceClient.create_from_connection_string(
            self.primary_connection_string, websockets=self.mqtt_over_websocket)
        # Connect the device client.
        await self.client.connect()

    async def shutdown(self):
        """Disconnect the client from the Azure IoT Hub instance. Shut down the client for graceful exit."""
        if self.client.connected:
            await self.client.shutdown()
