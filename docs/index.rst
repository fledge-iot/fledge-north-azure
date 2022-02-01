.. Images
.. |azure| image:: images/azure.jpg

Azure IoT Hub
=============

The *fledge-north-azure* plugin sends data from Fledge to the Microsoft Azure IoT Core service.

The configuration of the *Azure* plugin requires a few simple configuration parameters to be set.

+---------+
| |azure| |
+---------+

  - **Primary Connection String**: The primary connection string to connect to your Azure IoT project. The connection string should contain 

     - The hostname to connect to 

     - The DeviceID of the device you are using 

     - The shared access key generated on your Azure login

  - **MQTT over websockets**: Enable if you wish to run MQTT over websockets.

  - **Data Source**: Which Fledge data to send to Azure; Readings or Fledge Statistics.

