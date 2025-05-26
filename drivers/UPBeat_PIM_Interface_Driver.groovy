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
#include UPBeat.UPBProtocolLib

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

@Field static final byte WRITE_REGISTER = 0x17
@Field static final byte READ_REGISTER = 0x12
@Field static final byte TRANSMIT_MESSAGE = 0x14
@Field static final byte EOM = 0x0D

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
    logTrace("setDeviceStatus(state=%s,reason=%s,forceEvent=%s)", state, reason, forceEvent)
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
        logInfo("Removing %s mutex", device.deviceNetworkId)
        deviceMutexes.remove(device.deviceNetworkId)
        logInfo("Removing %s response buffer", device.deviceNetworkId)
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
            logError("Socket Status: %s", message)
            setDeviceStatus("error", message, true)
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
    logTrace("parse(%s)", hexMessage)
    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    deviceResponses.putIfAbsent(device.deviceNetworkId, [response: 'None', data: null, semaphore: new Semaphore(0)])

    try {
        isCorrectParent()
        logDebug("Message Received: [${hexMessage}]")

        byte[] messageBytes = HexUtils.hexStringToByteArray(hexMessage)

        if (messageBytes.size() > 0 && messageBytes[messageBytes.length - 1] == EOM) {
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

        def asciiMessage = new String(messageBytes)
        logTrace("[%s]: [%s] (Bytes to String)", HexUtils.byteArrayToHexString(messageBytes), asciiMessage)

        byte[] messageTypeBytes = messageBytes[0..1]
        String messageType = new String(messageTypeBytes)
        logTrace("[%s]: Type=[%s]", asciiMessage, messageType)

        byte[] messageData = new byte[0]
        if (messageBytes.size() > 2) {
            messageData = messageBytes[2..-1]
            if (messageType == 'PU') {
                String messageDataString = new String(messageData)
                try {
                    messageData = HexUtils.hexStringToByteArray(messageDataString)
                    logTrace("[%s]: PU Data=%s", asciiMessage, HexUtils.byteArrayToHexString(messageData))
                } catch (Exception e) {
                    logError("[%s]: Failed to parse PU data: ${e.message}")
                    setDeviceStatus("error", "PU data parsing failed: ${e.message}", true)
                    return
                }
            } else {
                logTrace("[%s]: Data=%s", asciiMessage, HexUtils.byteArrayToHexString(messageData))
            }
        }

        def responseEntry = deviceResponses.get(device.deviceNetworkId)
        synchronized (responseEntry) {
            switch (messageType) {
                case 'PA':
                    responseEntry.response = 'PA'
                    logDebug("PIM accept message (PA)")
                    responseEntry.semaphore.release()
                    break
                case 'PE':
                    responseEntry.response = 'PE'
                    logError("PIM error message (PE)")
                    responseEntry.semaphore.release()
                    break
                case 'PB':
                    responseEntry.response = 'PB'
                    logWarn("PIM busy message (PB)")
                    responseEntry.semaphore.release()
                    break
                case 'PK':
                    responseEntry.response = 'PK'
                    logDebug("PIM ACK response (PK)")
                    responseEntry.semaphore.release()
                    break
                case 'PN':
                    responseEntry.response = 'PN'
                    logDebug("PIM NAK response (PN)")
                    responseEntry.semaphore.release()
                    break
                case 'PR':
                    responseEntry.response = 'PR'
                    responseEntry.data = messageData
                    logDebug("PIM register report message")
                    responseEntry.semaphore.release()
                    break
                case 'PU':
                    logDebug("UPB report message")
                    runIn(0, "asyncParseMessageReport", [data: [messageData: messageData]])
                    break
                default:
                    logError("Unknown message type: ${messageType}")
                    setDeviceStatus("error", "Message parsing failed: Unknown message type: ${messageType}", true)
                    return
            }
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
    logTrace("openSocket()")
    try {
        isCorrectParent()
        try {
            interfaces.rawSocket.connect(settings.ipAddress, settings.portNumber.toInteger(), byteInterface: true, eol: '\r')
            logInfo("Connected to %s:%s", ipAddress, portNumber)
            setNetworkStatus("Connected")
            setDeviceStatus("ok")
            return true
        } catch (Exception e) {
            logError("Connect failed to %s:%s - %s", ipAddress, portNumber, e.getMessage())
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
    logTrace("closeSocket()")
    try {
        interfaces.rawSocket.close()
        setNetworkStatus("Disconnected")
        setModuleStatus("Inactive")
        setDeviceStatus("ok")
        return true
    } catch (Exception e) {
        logWarn("Disconnect failed")
        setNetworkStatus("Disconnect failed", e.getMessage())
        setModuleStatus("Inactive", "Failed to disconnect from socket")
        setDeviceStatus("error", "Failed to disconnect from socket: ${e.getMessage()}", true)
        return false
    }
}

def reconnectSocket() {
    logTrace("reconnectSocket()")
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
    logTrace("sendBytes(%s)", bytes)
    def hexString = HexUtils.byteArrayToHexString(bytes)
    interfaces.rawSocket.sendMessage(hexString)
}

def setIPAddress(String ipAddress) {
    logTrace("setIPAddress(%s)", ipAddress)
    try {
        isCorrectParent()
        logInfo("Setting IP address to ${ipAddress}")
        device.updateSetting("ipAddress", [value: ipAddress, type: "text"])
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def setPortNumber(int portNumber) {
    logTrace("setPortNumber(%d)", portNumber)
    try {
        isCorrectParent()
        logInfo("Setting port number to %d", portNumber)
        device.updateSetting("portNumber", [value: portNumber, type: "number"])
        setDeviceStatus("ok")
    } catch (IllegalStateException e) {
        log.error e.message
        setModuleStatus("Inactive", e.message)
        setDeviceStatus("error", e.message, true)
    }
}

def setPIMCommandMode() {
    logTrace("setPIMCommandMode()")
    if (writePimRegister((byte) 0x70, [0x02] as byte[])) {
        logInfo("PIM was successfully set to Message Mode.")
        setModuleStatus("Active")
        setDeviceStatus("ok")
        return true
    } else {
        logWarn("PIM failed to set to Message Mode.")
        setModuleStatus("Inactive")
        setDeviceStatus("error", "Failed to set PIM to Message Mode", true)
        return false
    }
}

def readPimRegister(byte register, int numRegisters) {
    logTrace("readPimRegister(register=0x%02X, numRegisters=%d)", register, numRegisters)
    if (numRegisters < 1 || numRegisters > 16) {
        logError("Invalid number of registers: %d (must be 1-16)", numRegisters)
        return 'Failed'
    }

    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    deviceResponses.putIfAbsent(device.deviceNetworkId, [response: 'None', data: null, semaphore: new Semaphore(0)])

    // Build packet: [register, numRegisters]
    def packet = new ByteArrayOutputStream()
    packet.write([register, numRegisters] as byte[])
    byte sum = checksum(packet.toByteArray())
    packet.write(sum)
    byte[] encodedPacket = HexUtils.byteArrayToHexString(packet.toByteArray()).getBytes()
    logDebug("Encoded PIM read packet: %s", new String(encodedPacket))

    def message = new ByteArrayOutputStream()
    message.write(READ_REGISTER)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytes = message.toByteArray()

    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def retry = 0
        def maxIterations = maxRetry + 10
        def iteration = 0
        def finalResult = 'Failed'
        def resultData = null

        while (iteration++ < maxIterations) {
            def responseEntry = deviceResponses.get(device.deviceNetworkId)
            responseEntry.response = 'None'
            responseEntry.data = null
            responseEntry.semaphore.drainPermits()
            sendBytes(bytes)
            logDebug("Read register message sent to PIM: %s (register=0x%02X, numRegisters=%d)", new String(encodedPacket), register, numRegisters)

            if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                def response = responseEntry.response
                if (response == 'PA') {
                    logDebug("PIM accepted read register message (PA)")
                    responseEntry.semaphore.drainPermits()
                    if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                        response = responseEntry.response
                        if (response == 'PR') {
                            if (responseEntry.data?.length >= 2 + numRegisters) {
                                // PR response: PRRRVV, skip RR (2 bytes)
                                resultData = responseEntry.data[2..(1 + numRegisters)]
                                logDebug("Received register report (PR) with data: %s", HexUtils.byteArrayToHexString(resultData))
                                finalResult = 'Success'
                            } else {
                                logError("Invalid PR response data length: %d (expected >= %d)", responseEntry.data?.length ?: 0, 2 + numRegisters)
                            }
                            break
                        }
                    } else {
                        logError("Timeout waiting for PR response")
                    }
                } else if (response == 'PE') {
                    logError("PIM rejected read register message (PE)")
                    break
                } else if (response == 'PB' && retry++ < maxRetry) {
                    logWarn("PIM busy (PB), retrying (%d/%d)", retry + 1, maxRetry)
                    pauseExecution(100)
                    continue
                }
            } else {
                logWarn("Timeout waiting for initial PA response, retrying")
                if (retry++ >= maxRetry) break
                pauseExecution(100)
            }
        }

        if (iteration >= maxIterations) {
            logError("Read register aborted: Maximum iterations reached")
        }
        return finalResult == 'Success' ? [result: finalResult, data: resultData] : finalResult
    }
}

def writePimRegister(byte register, byte[] values) {
    logTrace("writePimRegister(register=0x%02X, values=%s)", register, HexUtils.byteArrayToHexString(values))
    if (values.length < 1 || values.length > 16) {
        logError("Invalid number of values: %d (must be 1-16)", values.length)
        return false
    }

    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    deviceResponses.putIfAbsent(device.deviceNetworkId, [response: 'None', data: null, semaphore: new Semaphore(0)])

    // Build packet: [register, values...]
    def packet = new ByteArrayOutputStream()
    packet.write(register)
    packet.write(values)
    byte sum = checksum(packet.toByteArray())
    packet.write(sum)
    byte[] encodedPacket = HexUtils.byteArrayToHexString(packet.toByteArray()).getBytes()
    logDebug("Encoded PIM write packet: %s", new String(encodedPacket))

    def message = new ByteArrayOutputStream()
    message.write(WRITE_REGISTER)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytes = message.toByteArray()

    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def retry = 0
        def maxIterations = maxRetry + 10
        def iteration = 0
        def finalResult = false

        while (iteration++ < maxIterations) {
            def responseEntry = deviceResponses.get(device.deviceNetworkId)
            responseEntry.response = 'None'
            responseEntry.data = null
            responseEntry.semaphore.drainPermits()
            sendBytes(bytes)
            logDebug("Write register message sent to PIM: %s (register=0x%02X, values=%s)", new String(encodedPacket), register, values)

            if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                def response = responseEntry.response
                if (response == 'PA') {
                    logDebug("PIM accepted write register message (PA)")
                    finalResult = true
                    break
                } else if (response == 'PE') {
                    logError("PIM rejected write register message (PE)")
                    break
                } else if (response == 'PB' && retry++ < maxRetry) {
                    logWarn("PIM busy (PB), retrying (%d/%d)", retry + 1, maxRetry)
                    pauseExecution(100)
                    continue
                }
            } else {
                logWarn("Timeout waiting for PA response, retrying")
                if (retry++ >= maxRetry) break
                pauseExecution(100)
            }
        }

        if (iteration >= maxIterations) {
            logError("Write register aborted: Maximum iterations reached")
        }
        return finalResult
    }
}

def transmitMessage(byte[] upbMessage) {
    logTrace("transmitMessage(upbMessage=%s)", upbMessage)
    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    deviceResponses.putIfAbsent(device.deviceNetworkId, [response: 'None', data: null, semaphore: new Semaphore(0)])

    byte[] encodedPacket = encodePimPacket(upbMessage)

    def message = new ByteArrayOutputStream()
    message.write(TRANSMIT_MESSAGE)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytes = message.toByteArray()

    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def retry = 0
        def maxIterations = maxRetry + 10
        def iteration = 0
        def finalResult = 'Failed'

        while (iteration++ < maxIterations) {
            def responseEntry = deviceResponses.get(device.deviceNetworkId)
            responseEntry.response = 'None'
            responseEntry.data = null
            responseEntry.semaphore.drainPermits()
            sendBytes(bytes)
            logDebug("UPB message sent to PIM: %s", new String(encodedPacket))

            if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                def response = responseEntry.response
                if (response == 'PA') {
                    logDebug("PIM accepted UPB message (PA)")
                    responseEntry.semaphore.drainPermits()
                    if (responseEntry.semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                        response = responseEntry.response
                        if (response in ['PK', 'PN']) {
                            finalResult = response == 'PK' ? 'Success' : 'NoAck'
                            logDebug("Transmission completed with %s (%s)", response == 'PK' ? 'ACK' : 'NAK', response)
                            break
                        }
                    } else {
                        logError("Timeout waiting for PK/PN response")
                    }
                } else if (response == 'PE') {
                    logError("PIM rejected UPB message (PE)")
                    break
                } else if (response == 'PB' && retry++ < maxRetry) {
                    logWarn("PIM busy (PB), retrying (%d/%d)", retry + 1, maxRetry)
                    pauseExecution(100)
                    continue
                }
            } else {
                logWarn("Timeout waiting for initial PA response, retrying")
                if (retry++ >= maxRetry) break
                pauseExecution(100)
            }
        }

        if (iteration >= maxIterations) {
            logError("UPB message transmission aborted: Maximum iterations reached")
        }
        return finalResult
    }
}

def asyncParseMessageReport(Map data) {
    logTrace("asyncParseMessageReport()")
    try {
        def messageData = data?.messageData
        if (messageData) {
            processMessage(messageData.collect { it as byte } as byte[])
        } else {
            logWarn("No message data in asyncParseMessageReport")
        }
    } catch (Exception e) {
        logError("Error in asyncParseMessageReport: ${e.message}")
    }
}

def processMessage(byte[] data) {
    logTrace("processMessage(%s)", data)
    def messageDataString = HexUtils.byteArrayToHexString(data)

    try {
        def packet = parsePacket(data)
        def controlWord = packet.controlWord
        def networkId = packet.networkId
        def destinationId = packet.destinationId
        def sourceId = packet.sourceId
        def messageDataId = packet.messageDataId
        def messageSetId = packet.messageSetId
        def messageArgs = packet.messageArgs

        logDebug("Handling %s (0x%02X): controlWord=0x%04X, networkId=0x%02X, sourceId=0x%02X, destinationId=0x%02X, messageDataId=0x%02X, messageArgs=%s",
                getMsidName(messageSetId), messageSetId, controlWord, networkId, sourceId, destinationId, messageDataId, messageArgs)

        def args = messageArgs.collect { it & 0xFF } as int[]
        switch (messageSetId) {
            case UPB_CORE_COMMAND:
                handleCommand(getMdidName(messageDataId), networkId, sourceId, destinationId, args)
                break
            case UPB_DEVICE_CONTROL_COMMAND:
                if (messageDataId in [UPB_ACTIVATE_LINK, UPB_DEACTIVATE_LINK]) {
                    getParent().handleLinkEvent("pim", getMdidName(messageDataId), networkId & 0xFF, sourceId & 0xFF, destinationId & 0xFF)
                } else if (messageDataId in [UPB_GOTO, UPB_REPORT_STATE]) {
                    getParent().handleDeviceEvent("pim", getMdidName(messageDataId), networkId & 0xFF, sourceId & 0xFF, destinationId & 0xFF, args)
                } else {
                    handleCommand(getMdidName(messageDataId), networkId, sourceId, destinationId, args)
                }
                break
            case UPB_CORE_REPORTS:
                if (messageDataId == UPB_DEVICE_STATE && args.size() >= 1) {
                    getParent().handleDeviceEvent("pim", getMdidName(messageDataId), networkId & 0xFF, sourceId & 0xFF, destinationId & 0xFF, args)
                } else {
                    handleCommand(getMdidName(messageDataId), networkId, sourceId, destinationId, args)
                }
                break
            case UPB_EXTENDED_MESSAGE_SET:
                logWarn("Unsupported extended message: MDID=0x%02X", messageDataId)
                break
        }
    } catch (IllegalArgumentException e) {
        logError("[${messageDataString}]: ${e.message}")
    }
}

private void handleCommand(String commandName, byte networkId, byte sourceId, byte destinationId, int[] args) {
    logDebug("Handling %s: networkId=0x%02X, sourceId=0x%02X, destinationId=0x%02X, args=%s",
            commandName, networkId, sourceId, destinationId, args)
}