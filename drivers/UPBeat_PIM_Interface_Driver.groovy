/*
* Hubitat Driver: UPB Powerline Interface Module
* Description: Network driver for Universal Powerline Bus communication
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
import hubitat.helper.HexUtils
import groovy.transform.Field
import java.util.concurrent.ConcurrentHashMap
#include UPBeat.UPBeatLogger

metadata {
    definition(name: "UPB Powerline Interface Module", namespace: "UPBeat", author: "UPBeat Automation") {
        capability "Initialize"
        attribute "Network", "string"
        attribute "PIM", "string"
    }

    preferences {
        input name: "ipAddress", type: "text", title: "IP Address", description: "IP Address of Serial to Network Device", required: true
        input name: "portNumber", type: "number", title: "Port", description: "Port of Serial to Network Device", required: true, range: 0..65535
        input name: "maxRetry", type: "number", title: "Retries", description: "Number of retries for PIM busy messages", required: true, range: 1..60, defaultValue: 10
        input name: "maxProcessingTime", type: "number", title: "Timeout", description: "Timeout for messages being sent", required: true, range: 0..60000, defaultValue: 10000
        input name: "reconnectInterval", type: "number", title: "Reconnect", description: "Reconnect Interval", required: true, range: 0..60000, defaultValue: 60
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
    }
}

/***************************************************************************
 * Global Static Data
 ***************************************************************************/
@Field static ConcurrentHashMap deviceMutexes = new ConcurrentHashMap()
@Field static ConcurrentHashMap deviceResponses = new ConcurrentHashMap()

// MSID Mapping
@Field static final byte UPB_CORE_COMMAND = 0x00
@Field static final byte UPB_DEVICE_CONTROL_COMMAND = 0x01
@Field static final byte UPB_RESERVED_COMAND_SET_1 = 0x02
@Field static final byte UPB_RESERVED_COMAND_SET_2 = 0x03
@Field static final byte UPB_CORE_REPORTS = 0x04
@Field static final byte UPB_RESERVED_REPORT_SET_1 = 0x05
@Field static final byte UPB_RESERVED_REPORT_SET_2 = 0x06
@Field static final byte UPB_EXTENDED_MESSAGE_SET = 0x07

// Core Commands
@Field static final byte UPB_NULL_COMMAND = 0x00
@Field static final byte UPB_WRITE_ENABLED_COMMAND = 0x01
@Field static final byte UPB_WRITE_PROTECT_COMMAND = 0x02
@Field static final byte UPB_START_SETUP_MODE_COMMAND = 0x03
@Field static final byte UPB_STOP_SETUP_MODE_COMMAND = 0x04
@Field static final byte UPB_GET_SETUP_TIME_COMMAND = 0x05
@Field static final byte UPB_AUTO_ADDRESS_COMMAND = 0x06
@Field static final byte UPB_GET_DEVICE_STATUS_COMMAND = 0x07
@Field static final byte UPB_SET_DEVICE_CONTROL_COMMAND = 0x08
// 0x09 – 0x0A Unused Reserved for future command use.
@Field static final byte UPB_ADD_LINK_COMMAND = 0x0B
@Field static final byte UPB_DEL_LINK_COMMAND = 0x0C
@Field static final byte UPB_TRANSMIT_MESSAGE_COMMAND = 0x0D
@Field static final byte UPB_DEVICE_RESET_COMMAND = 0x0E
@Field static final byte UPB_GET_DEVICE_SIG_COMMAND = 0x0F
@Field static final byte UPB_GET_REGISTER_VALUE_COMMAND = 0x10
@Field static final byte UPB_SET_REGISTER_VALUE_COMMAND = 0x11
// 0x12 – 0x1F Unused Reserved for future command use.

// Device Control Commands
@Field static final byte UPB_ACTIVATE_LINK = 0x20
@Field static final byte UPB_DEACTIVATE_LINK = 0x21
@Field static final byte UPB_GOTO = 0x22
@Field static final byte UPB_FADE_START = 0x23
@Field static final byte UPB_FADE_STOP = 0x24
@Field static final byte UPB_BLINK = 0x25
@Field static final byte UPB_INDICATE = 0x26
@Field static final byte UPB_TOGGLE = 0x27
//0x28 – 0x2F // Reserved for future use
@Field static final byte UPB_REPORT_STATE = 0x30
@Field static final byte UPB_STORE_STATE = 0x31
//0x32 – 0x3F // Reserved for future use

// Core Report Set
@Field static final byte UPB_ACK_RESPONSE = 0x80
//0x81 – 0x84 //Unused
@Field static final byte UPB_SETUP_TIME = 0x85
@Field static final byte UPB_DEVICE_STATE = 0x86
@Field static final byte UPB_DEVICE_STATUS = 0x87
//0x88 – 0x8E // Unused
@Field static final byte UPB_DEVICE_SIG = 0x8F
@Field static final byte UPB_REGISTER_VALUES = 0x90
@Field static final byte UPB_RAM_VALUES = 0x91
@Field static final byte UPB_RAW_DATA = 0x92
@Field static final byte UPB_HEARTBEAT = 0x93
//0x94 – 0x9F // Unused
/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
def installed() {
    logTrace "installed()"
}

def uninstalled() {
    logTrace "uninstalled()"
    closeSocket()
    logInfo "Removing ${device.deviceNetworkId} mutex"
    deviceMutexes.remove(device.deviceNetworkId)
    logInfo "Removing ${device.deviceNetworkId} response buffer"
    deviceResponses.remove(device.deviceNetworkId)
}

def updated() {
    logTrace "updated()"
    initialize()
}

def initialize(){
    logTrace "initialize()"
    logInfo "DNI: ${device.deviceNetworkId}"
    logInfo "Host: ${ipAddress}:${portNumber}"
    logInfo "maxRetry: ${maxRetry}"
    logInfo "maxProcessingTime: ${maxProcessingTime}"
    logInfo "reconnectInterval: ${reconnectInterval}"
    logInfo "LogLevel: [${LOG_LEVELS[logLevel.toInteger()]}]"
    deviceMutexes.put(device.deviceNetworkId, new Object())
    deviceResponses.put(device.deviceNetworkId, new String())
    closeSocket()
    openSocket()
    setPIMCommandMode()
}

/***************************************************************************
 * Web Socket User Defined
 ***************************************************************************/
def socketStatus(message) {
    logTrace "socketStatus()"
    logDebug "Message: ${message}"
    if (message.contains('error: Stream closed') || message.contains('error: Connection timed out') || message.contains("receive error: Connection reset")) {
        logError "socketStatus(): ${message}"
        closeSocket()
        runIn(reconnectInterval, reconnectSocket)
    }
    return
}

def parse(hexMessage) {
    logTrace "parse()"
    logDebug "Message Received: [${hexMessage}]"

    byte[] messageBytes = hubitat.helper.HexUtils.hexStringToByteArray(hexMessage)

    // Strip EOL character, should be present always
    if (messageBytes.size() > 0 && messageBytes[messageBytes.length - 1] == 0x0D) {
        messageBytes = messageBytes[0..-2]
        logDebug("[${hexMessage}]: ${hubitat.helper.HexUtils.byteArrayToHexString(messageBytes)} (EOL Removed)")
    } else {
        logError "[${hexMessage}]: No EOL found"
    }

    if (messageBytes.size() < 2) {
        logError "[${hexMessage}]: Invalid data"
        return
    }

    // Show converted and parsed type
    def asciiMessage = new String(messageBytes)
    logDebug "[${hubitat.helper.HexUtils.byteArrayToHexString(messageBytes)}]: [${asciiMessage}] (Converted)"

    // Parse message type from original bytes
    byte[] messageTypeBytes = messageBytes[0..1]
    String messageType = new String(messageTypeBytes)

    logDebug "[${asciiMessage}]: Type=[${messageType}]"

    byte[] messageData = new byte[0]

    if (messageBytes.size() > 2) {
        messageData = messageBytes[2..-1]
        String messageDataString = new String(messageData)
        messageData = hubitat.helper.HexUtils.hexStringToByteArray(messageDataString)
        logDebug "[${asciiMessage}]: Data=${messageDataString}"
    }

    switch (messageType) {
        case "PA":
            deviceResponses.put(device.deviceNetworkId, 'PA')
            logDebug "pim_accept_message"
            break
        case "PE":
            deviceResponses.put(device.deviceNetworkId, 'PE')
            logError "pim_error_message"
            break
        case "PB":
            deviceResponses.put(device.deviceNetworkId, 'PB')
            logWarn "pim_busy_message"
            break
        case "PK":
            logDebug "upb_ack_message"
            break
        case "PN":
            logWarn "upb_nak_message"
            break
        case "PR":
            logDebug "pim_register_report_message"
            break
        case "PU":
            logDebug "upb_report_message"
            parseMessageReport(messageData)
            break
        default:
            logError "Unknown message type"
            break
    }
}

/***************************************************************************
 * Custom Driver Functions
 ***************************************************************************/

def openSocket() {
    logTrace "openSocket()"
    try {
        interfaces.rawSocket.connect(settings.ipAddress, settings.portNumber.toInteger(), byteInterface: true, eol: '\r')
        logInfo "Connected to ${settings.ipAddress}:${settings.portNumber}"
        setNetworkStatus("Connected")
        return true
    }
    catch (Exception e) {
        logError "Connect failed to ${settings.ipAddress}:${settings.portNumber} - ${e.getMessage()}"
        setNetworkStatus("Connect failed", e.getMessage())
    }
    return false
}

def closeSocket() {
    logTrace "closeSocket()"
    try {
        interfaces.rawSocket.close()
        logInfo "Disconnected from ${settings.ipAddress}:${settings.portNumber}"
        setNetworkStatus("Disconnected")
        return true
    }
    catch (Exception e) {
        logWarn "Disconnected failed from ${settings.ipAddress}:${settings.portNumber}"
        setNetworkStatus("Disconnect failed", e.getMessage())
    }
    return false
}

def reconnectSocket() {
    logTrace "reconnectSocket()"
    openSocket()
    setPIMCommandMode()
}

private def sendBytes(byte[] bytes) {
    logTrace "sendBytes()"
    def hexString = hubitat.helper.HexUtils.byteArrayToHexString(bytes)
    interfaces.rawSocket.sendMessage(hexString)
}

private def checksum(byte[] data) {
    logTrace "checksum()"
    def sum = data.sum()
    return (~sum + 1) & 0xFF
}

private void setNetworkStatus(String state, String reason = '') {
    logTrace "setNetworkStatus()"
    String msg = "${device} is ${state.toLowerCase()}${(reason) ? ' :' + reason : ''}"
    sendEvent([name: "Network", value: state, descriptionText: msg, isStateChange: true])
    logInfo msg
}

private void setModuleStatus(String state, String reason = '') {
    logTrace "setModuleStatus()"
    String msg = "${device} is ${state.toLowerCase()}${(reason) ? ' :' + reason : ''}"
    sendEvent([name: "PIM", value: state, descriptionText: msg, isStateChange: true])
    logInfo msg
}

def setIPAddress(String ipAddress) {
    logTrace "setIPAddress()"
    logInfo "Setting IP address to ${ipAddress}"
    device.updateSetting("ipAddress", [value: ipAddress, type: "text"])
}

def setPortNumber(int portNumber) {
    logTrace "setPortNumber()"
    logInfo "Setting port number to ${portNumber}"
    device.updateSetting("portNumber", [value: portNumber, type: "number"])
}

def getCommandModeMessage() {
    logTrace "getCommandModeMessage()"

    def packet = new ByteArrayOutputStream()
    packet.write([0x70, 0x02] as byte[]) // Control Word
    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: ${String.format("%02X", sum & 0xFF)}"
    packet.write(sum)

    String packetTextHex = HexUtils.byteArrayToHexString(packet.toByteArray())

    logDebug "PIM Packet: ${packetTextHex}"

    byte[] encodedPacket = packetTextHex.getBytes()

    message = new ByteArrayOutputStream()
    message.write(0x17) // Write Register
    message.write(encodedPacket) // UPB Message + Checksum
    message.write(0x0D) // EOL
    def pimBytes = message.toByteArray()

    logDebug "PIM Message Encoded: ${HexUtils.byteArrayToHexString(pimBytes)}"

    return pimBytes
}

def setPIMCommandMode() {
    logTrace "setPIMCommandMode()"
    // Hex encoded string 0x17 + 70028E + 0x0D (Command Mode)
    if (transmitMessage(getCommandModeMessage())) {
        logInfo "PIM was successfully set to command mode."
        setModuleStatus("Active")
    } else {
        logWarn "PIM failed to set to command mode."
        setModuleStatus("Inactive")
    }
}

def transmitMessage(byte[] bytes) {
    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        logTrace "transmitMessage()"
        long retry = 0
        boolean exit = false
        String sendStatus = 'None'
        deviceResponses.put(device.deviceNetworkId, 'None')

        def startTime = now()

        while (!exit) {
            switch (sendStatus) {
                case 'None':
                    sendStatus = 'Sent'
                    sendBytes(bytes)
                    logDebug "Sent UPB Message"
                    break
                case 'Sent':
                    switch (deviceResponses.get(device.deviceNetworkId)) {
                        case "PA":
                            sendStatus = 'Success'
                            break
                        case "PE":
                            sendStatus = 'Failed'
                            break
                        case "PB":
                            sendStatus = 'Retry'
                            break
                        default: // None etc
                            break
                    }
                    break
                case 'Retry':
                    if (retry++ < maxRetry) {
                        logWarn "Retrying to send message."
                        sendBytes(bytes)
                        sendStatus = 'Sent'
                    } else {
                        logError "Retry limit reached."
                        sendStatus = 'Failed'
                    }
                    break
                case 'Failed':
                    logError "UPB Message could not be sent."
                    exit = true
                    break
                case 'Success':
                    logInfo "UPB Message sent successfully."
                    exit = true
                    break
            }

            if ((now() - startTime) > maxProcessingTime) {
                sendStatus = 'Failed'
            }
            pauseExecution(200)
        }

        if (deviceResponses.get(device.deviceNetworkId) == 'PA')
            return true
        else
            return false
    }
}


def parseMessageReport(byte[] data) {
    logTrace "parseMessageReport()"
    def messageDataString = HexUtils.byteArrayToHexString(data)

    // Validate packet length
    if (data.size() < 5) {
        logError "[${messageDataString}]: Invalid UPB packet, too short"
        return
    }

    // Validate checksum
    int sum = 0
    data.each { b -> sum += (b & 0xFF) }
    if ((sum & 0xFF) != 0) {
        logError "[${messageDataString}]: Invalid checksum CHK=${sum}"
        return
    }

    // Parse 5-byte UPB header
    short controlWord = (short) (((data[0] & 0xFF) << 8) | (data[1] & 0xFF))
    byte networkId = data[2]
    byte destinationId = data[3]
    byte sourceId = data[4]
    logDebug "[${messageDataString}]: HDR Control=${String.format('0x%04X (%d)', controlWord, controlWord)}, NID=${String.format('0x%02X (%d)', networkId & 0xFF, networkId & 0xFF)}, DID=${String.format('0x%02X (%d)', destinationId & 0xFF, destinationId & 0xFF)}, SID=${String.format('0x%02X (%d)', sourceId & 0xFF, sourceId & 0xFF)}"

    // Parse UPB message
    byte[] messageContent = data[5..-2]
    if (messageContent.size() < 1) {
        logError "[${messageDataString}]: No message data"
        return
    }

    byte messageDataId = messageContent[0]
    logDebug "[${messageDataString}]: MDID=${String.format('0x%02X', messageDataId)}"

    byte messageSetId = (messageDataId >> 5) & 0x07
    byte messageIdByte = messageDataId & 0x1F
    byte[] messageArgs = messageContent[1..-1]
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    logDebug "[${messageDataString}]: MDA=${argsHex}"

    switch(messageSetId) {
        case UPB_CORE_COMMAND:
            logDebug "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            processCoreCommand(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_DEVICE_CONTROL_COMMAND:
            logDebug "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            processDeviceControlCommand(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_RESERVED_COMAND_SET_1:
            logWarn "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            break
        case UPB_RESERVED_COMAND_SET_2:
            logWarn "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            break
        case UPB_CORE_REPORTS:
            logDebug "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            processCoreReport(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_RESERVED_REPORT_SET_1:
            logWarn "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            break
        case UPB_RESERVED_REPORT_SET_2:
            logWarn "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            break
        case UPB_EXTENDED_MESSAGE_SET:
            logDebug "[${messageDataString}]: Handling ${getMsidName(messageSetId)}"
            processExtendedMessage(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        default:
            logError "[${messageDataString}]: Handling ${getMsidName(messageSetId)} ${String.format('0x%02X', messageSetId)}"
            break
    }
}

void processCoreCommand(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace "processCoreCommand()"
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    switch(messageDataId) {
        case UPB_NULL_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_WRITE_ENABLED_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_WRITE_PROTECT_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_START_SETUP_MODE_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_STOP_SETUP_MODE_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_GET_SETUP_TIME_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_AUTO_ADDRESS_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_GET_DEVICE_STATUS_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_SET_DEVICE_CONTROL_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_ADD_LINK_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_DEL_LINK_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_TRANSMIT_MESSAGE_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_DEVICE_RESET_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_GET_DEVICE_SIG_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_GET_REGISTER_VALUE_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_SET_REGISTER_VALUE_COMMAND:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        default:
            logError "Handling ${getMdidName(messageDataId)} ${String.format('0x%02X', messageDataId)}"
            break
    }
}

void processDeviceControlCommand(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace "processDeviceControlCommand()"
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    switch(messageDataId) {
        case UPB_ACTIVATE_LINK:
            sendEvent(name: "linkEvent", value: new groovy.json.JsonOutput().toJson([
                eventType: "activate",
                networkId: networkId & 0xFF,
                sourceId: sourceId & 0xFF,
                linkId: destinationId & 0xFF
            ]))
            logInfo "Generated linkEvent: activate, Network=${networkId & 0xFF}, Source=${sourceId & 0xFF}, Link=${destinationId & 0xFF}"
            break
        case UPB_DEACTIVATE_LINK:
            sendEvent(name: "linkEvent", value: new groovy.json.JsonOutput().toJson([
                eventType: "deactivate",
                networkId: networkId & 0xFF,
                sourceId: sourceId & 0xFF,
                linkId: destinationId & 0xFF
            ]))
            logInfo "Generated linkEvent: deactivate, Network=${networkId & 0xFF}, Source=${sourceId & 0xFF}, Link=${destinationId & 0xFF}"
            break
        case UPB_GOTO:
            int level = messageArgs.size() > 0 ? Math.min(messageArgs[0] & 0xFF, 100) : 0
            sendEvent(name: "deviceState", value: new groovy.json.JsonOutput().toJson([
                eventType: "goto",
                networkId: networkId & 0xFF,
                sourceId: sourceId & 0xFF,
                destinationId: destinationId & 0xFF,
                level: level,
                args: messageArgs.collect { it & 0xFF }
            ]))
            logInfo "Generated deviceState: Network=${networkId & 0xFF}, Source=${sourceId & 0xFF}, Destination=${destinationId & 0xFF}, Level=${level}"
			break
        case UPB_FADE_START:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_FADE_STOP:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_BLINK:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_INDICATE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_TOGGLE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_REPORT_STATE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_STORE_STATE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        default:
            logError "Handling ${getMdidName(messageDataId)} ${String.format('0x%02X', messageDataId)}"
            break
    }
}

void processCoreReport(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace "processCoreReport()"
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    switch(messageDataId) {
        case UPB_ACK_RESPONSE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_SETUP_TIME:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_DEVICE_STATE:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
			if (messageArgs.size() < 1) {
                logError "[${messageDataString}]: No state data in Device State Report"
                return
            }
            int level = Math.min(messageArgs[0] & 0xFF, 100)
            sendEvent(name: "deviceState", value: new groovy.json.JsonOutput().toJson([
                eventType: "state_report",
                networkId: networkId & 0xFF,
                sourceId: sourceId & 0xFF,
                destinationId: destinationId & 0xFF,
                level: level,
                args: messageArgs.collect { it & 0xFF }
            ]))
            logInfo "Generated deviceState: Network=${networkId & 0xFF}, Source=${sourceId & 0xFF}, Destination=${destinationId & 0xFF}, Level=${level}"
            break
        case UPB_DEVICE_STATUS:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_DEVICE_SIG:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_REGISTER_VALUES:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_RAM_VALUES:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_RAW_DATA:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        case UPB_HEARTBEAT:
            logDebug "Handling ${getMdidName(messageDataId)} Args=${argsHex}"
            break
        default:
            logError "Handling ${getMdidName(messageDataId)} ${String.format('0x%02X', messageDataId)}"
            break
    }
}

void processExtendedMessage(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace "processExtendedMessage()"
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    logDebug "Handling Extended Message: MDID=0x${String.format('%02X', messageDataId)}, Args=${argsHex}"
}

// Helper methods for readable logging
private String getMsidName(int msid) {
    switch (msid) {
        case UPB_CORE_COMMAND: return "UPB_CORE_COMMAND"
        case UPB_DEVICE_CONTROL_COMMAND: return "UPB_DEVICE_CONTROL_COMMAND"
        case UPB_RESERVED_COMAND_SET_1: return "UPB_RESERVED_COMAND_SET_1"
        case UPB_RESERVED_COMAND_SET_2: return "UPB_RESERVED_COMAND_SET_2"
        case UPB_CORE_REPORTS: return "UPB_CORE_REPORTS"
        case UPB_RESERVED_REPORT_SET_1: return "UPB_RESERVED_REPORT_SET_1"
        case UPB_RESERVED_REPORT_SET_2: return "UPB_RESERVED_REPORT_SET_2"
        case UPB_EXTENDED_MESSAGE_SET: return "UPB_EXTENDED_MESSAGE_SET"
        default: return "UNKNOWN_MSID"
    }
}

private String getMdidName(int mdid) {
    switch (mdid) {
        case UPB_NULL_COMMAND: return "UPB_NULL_COMMAND"
        case UPB_WRITE_ENABLED_COMMAND: return "UPB_WRITE_ENABLED_COMMAND"
        case UPB_WRITE_PROTECT_COMMAND: return "UPB_WRITE_PROTECT_COMMAND"
        case UPB_START_SETUP_MODE_COMMAND: return "UPB_START_SETUP_MODE_COMMAND"
        case UPB_STOP_SETUP_MODE_COMMAND: return "UPB_STOP_SETUP_MODE_COMMAND"
        case UPB_GET_SETUP_TIME_COMMAND: return "UPB_GET_SETUP_TIME_COMMAND"
        case UPB_AUTO_ADDRESS_COMMAND: return "UPB_AUTO_ADDRESS_COMMAND"
        case UPB_GET_DEVICE_STATUS_COMMAND: return "UPB_GET_DEVICE_STATUS_COMMAND"
        case UPB_SET_DEVICE_CONTROL_COMMAND: return "UPB_SET_DEVICE_CONTROL_COMMAND"
        case UPB_ADD_LINK_COMMAND: return "UPB_ADD_LINK_COMMAND"
        case UPB_DEL_LINK_COMMAND: return "UPB_DEL_LINK_COMMAND"
        case UPB_TRANSMIT_MESSAGE_COMMAND: return "UPB_TRANSMIT_MESSAGE_COMMAND"
        case UPB_DEVICE_RESET_COMMAND: return "UPB_DEVICE_RESET_COMMAND"
        case UPB_GET_DEVICE_SIG_COMMAND: return "UPB_GET_DEVICE_SIG_COMMAND"
        case UPB_GET_REGISTER_VALUE_COMMAND: return "UPB_GET_REGISTER_VALUE_COMMAND"
        case UPB_SET_REGISTER_VALUE_COMMAND: return "UPB_SET_REGISTER_VALUE_COMMAND"
        case UPB_ACTIVATE_LINK: return "UPB_ACTIVATE_LINK"
        case UPB_DEACTIVATE_LINK: return "UPB_DEACTIVATE_LINK"
        case UPB_GOTO: return "UPB_GOTO"
        case UPB_FADE_START: return "UPB_FADE_START"
        case UPB_FADE_STOP: return "UPB_FADE_STOP"
        case UPB_BLINK: return "UPB_BLINK"
        case UPB_INDICATE: return "UPB_INDICATE"
        case UPB_TOGGLE: return "UPB_TOGGLE"
        case UPB_REPORT_STATE: return "UPB_REPORT_STATE"
        case UPB_STORE_STATE: return "UPB_STORE_STATE"
        case UPB_ACK_RESPONSE: return "UPB_ACK_RESPONSE"
        case UPB_SETUP_TIME: return "UPB_SETUP_TIME"
        case UPB_DEVICE_STATE: return "UPB_DEVICE_STATE"
        case UPB_DEVICE_STATUS: return "UPB_DEVICE_STATUS"
        case UPB_DEVICE_SIG: return "UPB_DEVICE_SIG"
        case UPB_REGISTER_VALUES: return "UPB_REGISTER_VALUES"
        case UPB_RAM_VALUES: return "UPB_RAM_VALUES"
        case UPB_RAW_DATA: return "UPB_RAW_DATA"
        case UPB_HEARTBEAT: return "UPB_HEARTBEAT"
        default: return "UNKNOWN_MDID"
    }
}