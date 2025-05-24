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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatDriverLib

metadata {
    definition(name: "UPB Powerline Interface Module", namespace: "UPBeat", author: "UPBeat Automation") {
        capability "Initialize"
        attribute "Network", "string"
        attribute "PIM", "string"
        attribute "status", "enum", ["ok", "error"]
    }

    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        input name: "ipAddress", type: "text", title: "IP Address", description: "IP Address of Serial to Network Device", required: true
        input name: "portNumber", type: "number", title: "Port", description: "Port of Serial to Network Device", required: true, range: 0..65535
        input name: "maxRetry", type: "number", title: "Retries", description: "Number of retries for PIM busy messages", required: true, range: 1..60, defaultValue: 10
        input name: "maxProcessingTime", type: "number", title: "Timeout", description: "Timeout for messages being sent", required: true, range: 0..60000, defaultValue: 10000
        input name: "reconnectInterval", type: "number", title: "Reconnect", description: "Reconnect Interval", required: true, range: 0..60000, defaultValue: 60
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
 * Helper Functions
 ***************************************************************************/
private void setNetworkStatus(String state, String reason = '') {
    logTrace("setNetworkStatus(state=%s,reason=%s)", state, reason)
    String msg = "${device} is ${state.toLowerCase()}${reason ? ' :' + reason : ''}"
    sendEvent(name: "Network", value: state, descriptionText: msg, isStateChange: true)
}

private void setModuleStatus(String state, String reason = '') {
    logTrace("setModuleStatus(state=%s,reason=%s)", state, reason)
    String msg = "${device} is ${state.toLowerCase()}${reason ? ' :' + reason : ''}"
    sendEvent(name: "PIM", value: state, descriptionText: msg, isStateChange: true)
}

private void setDeviceStatus(String state, String reason = '', boolean forceEvent = false) {
    logTrace("setModuleStatus(state=%s,reason=%s,forceEvent=%s)", state, reason, forceEvent)
    String msg = reason ?: "${device} is ${state.toLowerCase()}"
    sendEvent(name: "status", value: state, descriptionText: msg, isStateChange: forceEvent)
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
def installed() {
    logTrace("installed()")
    try {
        isCorrectParent()
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setDeviceStatus("error", e.message, true)
    }
}

def uninstalled() {
    logTrace("uninstalled()")
    try {
        isCorrectParent()
        closeSocket()
        logInfo "Removing ${device.deviceNetworkId} mutex"
        deviceMutexes.remove(device.deviceNetworkId)
        logInfo "Removing ${device.deviceNetworkId} response buffer"
        deviceResponses.remove(device.deviceNetworkId)
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setDeviceStatus("error", e.message, true)
    }
}

def updated() {
    logTrace("updated()")
    try {
        isCorrectParent()
        initialize()
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def initialize() {
    logTrace("initialize()")
    try {
        isCorrectParent()
        logInfo("DNI: %s", device.deviceNetworkId)
        logInfo("hostInfo: %s:%s", ipAddress, portNumber)
        logInfo("maxRetry: %s", maxRetry)
        logInfo("maxProcessingTime: %s", maxProcessingTime)
        logInfo("reconnectInterval: %s", reconnectInterval)
        logInfo("logLevel: %s", LOG_LEVELS[logLevel.toInteger()])
        deviceMutexes.put(device.deviceNetworkId, new Object())
        deviceResponses.put(device.deviceNetworkId, [response: 'None', semaphore: new Semaphore(0)])

        closeSocket()
        if (!openSocket()) {
            setModuleStatus("Inactive", "Failed to open socket")
            throw new Exception("Failed to open socket during initialization")
        }
        if (!setPIMCommandMode()) {
            throw new Exception("Failed to set PIM command mode during initialization")
        }
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    } catch (Exception e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

/***************************************************************************
 * Web Socket User Defined
 ***************************************************************************/
def socketStatus(message) {
    logTrace("socketStatus(message=%s)", message)
    try {
        isCorrectParent()
        if (message.contains('error: Stream closed') || message.contains('error: Connection timed out') || message.contains("receive error: Connection reset")) {
            msg = sprintf("Socket Status: %s", message)
            logError(msg)
            setDeviceStatus("error", msg, true)
            logInfo("Attempting reconnect in %s seconds", reconnectInterval)
            runIn(reconnectInterval, reconnectSocket)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def parse(hexMessage) {
    logTrace("parse(%s)" , hexMessage)
    try {
        isCorrectParent()
        logDebug("Message Received: [${hexMessage}]")

        byte[] messageBytes = HexUtils.hexStringToByteArray(hexMessage)

        // Strip EOL character, should be present always
        if (messageBytes.size() > 0 && messageBytes[messageBytes.length - 1] == 0x0D) {
            messageBytes = messageBytes[0..-2]
            logTrace("[%s]: %s (EOL Removed)", hexMessage, HexUtils.byteArrayToHexString(messageBytes))
        } else {
            logError("[%s]: No EOL found", hexMessage)
            setDeviceStatus("error", "Message parsing failed: No EOL found", true)
            return
        }

        if (messageBytes.size() < 2) {
            logError("[%s]: Invalid data", hexMessage)
            setDeviceStatus("error", "Message parsing failed: Invalid data length", true)
            return
        }

        // Show converted and parsed type
        def asciiMessage = new String(messageBytes)
        logTrace("[%s]: [%s] (Bytes to String)", HexUtils.byteArrayToHexString(messageBytes), asciiMessage)

        // Parse message type from original bytes
        byte[] messageTypeBytes = messageBytes[0..1]
        String messageType = new String(messageTypeBytes)

        logTrace("[%s]: Type=[%s]", asciiMessage, messageType)

        byte[] messageData = new byte[0]

        if (messageBytes.size() > 2) {
            messageData = messageBytes[2..-1]
            String messageDataString = new String(messageData)
            messageData = HexUtils.hexStringToByteArray(messageDataString)
            logTrace("[%s]: Data=[%s]", asciiMessage, messageData)
        }

        def responseEntry = deviceResponses.get(device.deviceNetworkId)
        switch (messageType) {
            case "PA":
                synchronized (responseEntry) {
                    responseEntry.response = 'PA'
                    logDebug "pim_accept_message"
                    responseEntry.semaphore.release()
                }
                break
            case "PE":
                synchronized (responseEntry) {
                    responseEntry.response = 'PE'
                    logError "pim_error_message"
                    responseEntry.semaphore.release()
                }
                break
            case "PB":
                synchronized (responseEntry) {
                    responseEntry.response = 'PB'
                    logWarn "pim_busy_message"
                    responseEntry.semaphore.release()
                }
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
                // Dispatch to external thread, to prevent blocking
                runIn(0, "asyncParseMessageReport", [data: [messageData: messageData]])
                break
            default:
                logError "Unknown message type: ${messageType}"
                setDeviceStatus("error", "Message parsing failed: Unknown message type: ${messageType}", true)
                return
        }
        setModuleStatus("Active")
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

/***************************************************************************
 * Custom Driver Functions
 ***************************************************************************/
def openSocket() {
    logTrace "openSocket()"
    try {
        isCorrectParent()
        try {
            interfaces.rawSocket.connect(settings.ipAddress, settings.portNumber.toInteger(), byteInterface: true, eol: '\r')
            logInfo "Connected to ${settings.ipAddress}:${portNumber}"
            setNetworkStatus("Connected")
            setDeviceStatus("ok")
            return true
        } catch (Exception e) {
            logError "Connect failed to ${settings.ipAddress}:${portNumber} - ${e.getMessage()}"
            setNetworkStatus("Connect failed", e.getMessage())
            setModuleStatus("Inactive", "Failed to connect to socket")
            setDeviceStatus("error", "Failed to connect to socket: ${e.getMessage()}", true)
        }
        return false
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
        return false
    }
}

def closeSocket() {
    logTrace "closeSocket()"
    try {
        interfaces.rawSocket.close()
        logInfo "Disconnected from ${settings.ipAddress}:${portNumber}"
        setNetworkStatus("Disconnected")
        setModuleStatus("Inactive")
        setDeviceStatus("ok")
        return true
    } catch (Exception e) {
        logWarn "Disconnect failed from ${settings.ipAddress}:${portNumber}"
        setNetworkStatus("Disconnect failed", e.getMessage())
        setModuleStatus("Inactive", "Failed to disconnect from socket")
        setDeviceStatus("error", "Failed to disconnect from socket: ${e.getMessage()}", true)
        return false
    }
}

def reconnectSocket() {
    logTrace "reconnectSocket()"
    try {
        if (openSocket()) {
            if (setPIMCommandMode()) {
                setDeviceStatus("ok")
                return
            }
            throw new Exception("Failed to set PIM command mode during reconnect")
        }
        throw new Exception("Failed to open socket during reconnect")
    } catch (Exception e) {
        logError(e.message)
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
        logInfo("Attempting reconnect in %s seconds", reconnectInterval)
        runIn(reconnectInterval, reconnectSocket)
    }
}

private def sendBytes(byte[] bytes) {
    logTrace "sendBytes()"
    def hexString = HexUtils.byteArrayToHexString(bytes)
    interfaces.rawSocket.sendMessage(hexString)
}

private def checksum(byte[] data) {
    logTrace "checksum()"
    def sum = data.sum()
    return (~sum + 1) & 0xFF
}

def setIPAddress(String ipAddress) {
    logTrace "setIPAddress()"
    try {
        isCorrectParent()
        logInfo "Setting IP address to ${ipAddress}"
        device.updateSetting("ipAddress", [value: ipAddress, type: "text"])
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def setPortNumber(int portNumber) {
    logTrace "setPortNumber()"
    try {
        isCorrectParent()
        logInfo "Setting port number to ${portNumber}"
        device.updateSetting("portNumber", [value: portNumber, type: "number"])
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def getCommandModeMessage() {
    logTrace "getCommandModeMessage()"
    def packet = new ByteArrayOutputStream()
    packet.write([0x70, 0x02] as byte[]) // Control Word
    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: %02X", sum
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
    if (transmitMessage(getCommandModeMessage())) {
        logInfo "PIM was successfully set to command mode."
        setModuleStatus("Active")
        setDeviceStatus("ok")
        return true
    } else {
        logWarn "PIM failed to set to command mode."
        setModuleStatus("Inactive")
        setDeviceStatus("error", "Failed to set PIM to command mode", true)
        return false
    }
}

def transmitMessage(byte[] bytes) {
    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        logTrace "transmitMessage()"
        long retry = 0
        boolean exit = false
        String sendStatus = 'None'
        def responseEntry = deviceResponses.get(device.deviceNetworkId)

        while (!exit) {
            switch (sendStatus) {
                case 'None':
                    sendStatus = 'Sent'
                    responseEntry.response = 'None'
                    responseEntry.semaphore.drainPermits() // Reset permits to 0
                    sendBytes(bytes)
                    logDebug "Sent UPB Message"
                    break
                case 'Sent':
                    // Wait for the response to be set by parse()
                    if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                        def response = responseEntry.response
                        switch (response) {
                            case "PA":
                                sendStatus = 'Success'
                                break
                            case "PE":
                                sendStatus = 'Failed'
                                break
                            case "PB":
                                sendStatus = 'Retry'
                                break
                            default:
                                logError "PIM response is invalid ${response}"
                                sendStatus = 'Failed'
                                break
                        }
                    } else {
                        logError "Timeout waiting for PIM response"
                        sendStatus = 'Failed'
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
        }

        return responseEntry.response == 'PA'
    }
}

def asyncParseMessageReport(Map data) {
    logTrace "asyncParseMessageReport()"
    try {
        def messageData = data?.messageData
        if (messageData) {
            parseMessageReport(messageData.collect { it as byte } as byte[])
        } else {
            logWarn "No message data in asyncParseMessageReport"
        }
    } catch (Exception e) {
        logError "Error in asyncParseMessageReport: ${e.message}"
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
    byte sum = 0
    data.each { b -> sum += (b & 0xFF) } // Unsigned summation
    if (sum != 0) {
        logError("[%s]: Invalid checksum CHK=0x%02X (%hhu)", messageDataString, sum, sum)
        return
    }

    // Parse 5-byte UPB header
    short controlWord = (short) (((data[0] & 0xFF) << 8) | (data[1] & 0xFF))
    byte networkId = data[2]
    byte destinationId = data[3]
    byte sourceId = data[4]
    logTrace("[%s]: HDR Control=0x%04X (%d), NID=0x%02X (%hhu), DID=0x%02X (%hhu), SID=0x%02X (%hhu)",
            messageDataString, controlWord, controlWord,
            networkId, networkId,
            destinationId, destinationId,
            sourceId, sourceId)

    // Parse UPB message
    byte[] messageContent = data[5..-2]
    if (messageContent.size() < 1) {
        logError "[${messageDataString}]: No message data"
        return
    }

    byte messageDataId = messageContent[0]
    byte messageSetId = (messageDataId >> 5) & 0x07
    byte messageId = messageDataId & 0x1F

    logTrace("[%s]: MDID=0x%02X (%hhu), MSID=0x%02X (%hhu), MID=0x%02X (%hhu)",
            messageDataString,
            messageDataId, messageDataId,
            messageSetId,messageSetId,
            messageId,messageId)

    // Initialize messageArgs as an empty byte array
    byte[] messageArgs = new byte[0]
    if (messageContent.size() > 1) {
        messageArgs = messageContent[1..-1]
    }

    logTrace "[%s]: MDA=[%s]", messageDataString, messageArgs

    switch(messageSetId) {
        case UPB_CORE_COMMAND:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            processCoreCommand(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_DEVICE_CONTROL_COMMAND:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            processDeviceControlCommand(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_RESERVED_COMAND_SET_1:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            break
        case UPB_RESERVED_COMAND_SET_2:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            break
        case UPB_CORE_REPORTS:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            processCoreReport(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        case UPB_RESERVED_REPORT_SET_1:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            break
        case UPB_RESERVED_REPORT_SET_2:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            break
        case UPB_EXTENDED_MESSAGE_SET:
            logDebug("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            processExtendedMessage(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgs)
            break
        default:
            logError("Handling %s (0x%02X): controlWord=0x%04X (%d), networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s]",
                    getMsidName(messageSetId), messageSetId,
                    controlWord, controlWord,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageDataId, messageDataId,
                    messageArgs)
            break
    }
}

void processCoreCommand(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace("processCoreCommand(controlWord=0x%04X (%d), networkId=0x%02X (%hhu), destinationId=0x%02X (%hhu), sourceId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=%s)",
            controlWord, controlWord,
            networkId, networkId,
            destinationId, destinationId,
            sourceId, sourceId,
            messageDataId,messageDataId,
            messageArgs)

    switch(messageDataId) {
        case UPB_NULL_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_WRITE_ENABLED_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_WRITE_PROTECT_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_START_SETUP_MODE_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_STOP_SETUP_MODE_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_GET_SETUP_TIME_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_AUTO_ADDRESS_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_GET_DEVICE_STATUS_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_SET_DEVICE_CONTROL_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_ADD_LINK_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_DEL_LINK_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_TRANSMIT_MESSAGE_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_DEVICE_RESET_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_GET_DEVICE_SIG_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_GET_REGISTER_VALUE_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_SET_REGISTER_VALUE_COMMAND:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        default:
            logError("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
    }
}

void processDeviceControlCommand(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace("processDeviceControlCommand(controlWord=0x%04X (%d), networkId=0x%02X (%hhu), destinationId=0x%02X (%hhu), sourceId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s])",
            controlWord, controlWord,
            networkId, networkId,
            destinationId, destinationId,
            sourceId, sourceId,
            messageDataId,messageDataId,
            messageArgs)

    switch(messageDataId) {
        case UPB_ACTIVATE_LINK:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), linkId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            getParent().handleLinkEvent("pim",getMdidName(messageDataId),networkId & 0xFF,sourceId & 0xFF,destinationId & 0xFF)
            break
        case UPB_DEACTIVATE_LINK:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), linkId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            getParent().handleLinkEvent("pim",getMdidName(messageDataId),networkId & 0xFF,sourceId & 0xFF,destinationId & 0xFF)
            break
        case UPB_GOTO:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            int[] args = messageArgs.collect { it & 0xFF } as int[]
            getParent().handleDeviceEvent("pim",getMdidName(messageDataId),networkId & 0xFF,sourceId & 0xFF,destinationId & 0xFF,args)
            break
        case UPB_FADE_START:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_FADE_STOP:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_BLINK:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_INDICATE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_TOGGLE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_REPORT_STATE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_STORE_STATE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        default:
            logError("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
    }
}

void processCoreReport(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace("processCoreReport(controlWord=0x%04X (%d), networkId=0x%02X (%hhu), destinationId=0x%02X (%hhu), sourceId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s])",
            controlWord, controlWord,
            networkId, networkId,
            destinationId, destinationId,
            sourceId, sourceId,
            messageDataId,messageDataId,
            messageArgs)
    def argsHex = messageArgs.collect { String.format("0x%02X", it & 0xFF) }
    switch(messageDataId) {
        case UPB_ACK_RESPONSE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_SETUP_TIME:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_DEVICE_STATE:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            if (messageArgs.size() < 1) {
                logError "[${messageDataString}]: No state data in Device State Report"
                return
            }
            int[] args = messageArgs.collect { it & 0xFF } as int[]
            getParent().handleDeviceEvent("pim",getMdidName(messageDataId),networkId & 0xFF,sourceId & 0xFF,destinationId & 0xFF,args)
            break
        case UPB_DEVICE_STATUS:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_DEVICE_SIG:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_REGISTER_VALUES:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_RAM_VALUES:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_RAW_DATA:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        case UPB_HEARTBEAT:
            logDebug("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
        default:
            logError("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
                    getMdidName(messageDataId), messageDataId,
                    networkId, networkId,
                    sourceId, sourceId,
                    destinationId, destinationId,
                    messageArgs)
            break
    }
}

void processExtendedMessage(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgs) {
    logTrace("processExtendedMessage(controlWord=0x%04X (%d), networkId=0x%02X (%hhu), destinationId=0x%02X (%hhu), sourceId=0x%02X (%hhu), messageDataId=0x%02X (%hhu), messageArgs=[%s])",
            controlWord, controlWord,
            networkId, networkId,
            destinationId, destinationId,
            sourceId, sourceId,
            messageDataId,messageDataId,
            messageArgs)

    logError("Handling %s (0x%02X) networkId=0x%02X (%hhu), sourceId=0x%02X (%hhu), destinationId=0x%02X (%hhu), messageArgs=[%s]",
            getMdidName(messageDataId), messageDataId,
            networkId, networkId,
            sourceId, sourceId,
            destinationId, destinationId,
            messageArgs)
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