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
@Field static ConcurrentHashMap currentOutgoing = new ConcurrentHashMap()

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
        currentOutgoing.remove(device.deviceNetworkId)
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
        currentOutgoing.put(device.deviceNetworkId, [:])

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
            setNetworkStatus("Disconnected")
            setModuleStatus("Inactive")
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
    currentOutgoing.putIfAbsent(device.deviceNetworkId, [:])

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
            if (messageType == 'PU' || messageType == 'PR') {
                String messageDataString = new String(messageData)
                try {
                    messageData = HexUtils.hexStringToByteArray(messageDataString)
                    logTrace("[%s]: ${messageType} Data=%s", asciiMessage, HexUtils.byteArrayToHexString(messageData))
                } catch (Exception e) {
                    logError("[%s]: Failed to parse ${messageType} data: ${e.message}")
                    setDeviceStatus("error", "${messageType} data parsing failed: ${e.message}", true)
                    return
                }
            }
        }

        def outgoingMessage = currentOutgoing.get(device.deviceNetworkId)
        switch (messageType) {
            case 'PA':
                logDebug("PIM accept message (PA)")
                if (outgoingMessage.containsKey('pim_response')) {
                    logDebug("Outgoing message waiting on pim_response")
                    outgoingMessage.pim_response = 'PA'
                    outgoingMessage.pim_response_semaphore.release()
                }
                break
            case 'PE':
                logError("PIM error message (PE)")
                if (outgoingMessage.containsKey('pim_response')) {
                    logDebug("Outgoing message waiting on pim_response")
                    outgoingMessage.pim_response = 'PE'
                    outgoingMessage.pim_response_semaphore.release()
                }
                break
            case 'PB':
                logWarn("PIM busy message (PB)")
                if (outgoingMessage.containsKey('pim_response')) {
                    logDebug("Outgoing message waiting on pim_response")
                    outgoingMessage.pim_response = 'PB'
                    outgoingMessage.pim_response_semaphore.release()
                }
                break
            case 'PK':
                logDebug("PIM ACK response (PK)")
                if (outgoingMessage.containsKey('pim_ack')) {
                    outgoingMessage.pim_ack = 'PK'
                    outgoingMessage.pim_ack_semaphore.release()
                }
                break
            case 'PN':
                logDebug("PIM NAK response (PN)")
                if (outgoingMessage.containsKey('pim_ack')) {
                    outgoingMessage.pim_ack = 'PN'
                    outgoingMessage.pim_ack_semaphore.release()
                }
                break
            case 'PR':
                logDebug("PIM register report message: %s", messageData)
                def register_data = null
                try {
                    register_data = parseRegisterReport(messageData)
                } catch (IllegalArgumentException e) {
                    logError("Failed to parse PR message: %s", e.message)
                }

                if (outgoingMessage.containsKey('pim_response')) {
                    logDebug("Outgoing message waiting on pim_response")
                    outgoingMessage.pim_response = 'PR'
                    outgoingMessage.pim_response_data = register_data
                    outgoingMessage.pim_response_semaphore.release()
                }
                break
            case 'PU':
                logDebug("UPB Message Report")
                def packet = null
                try {
                    packet = parsePacket(messageData)
                    runIn(0, "asyncParseMessageReport", [data: [messageData: messageData]])
                } catch (IllegalArgumentException e) {
                    logError("parse Failed to parse PU message: %s", e.message)
                }

                if (outgoingMessage.containsKey('response_data')) {
                    logDebug("Stored UPB_DEVICE_STATE report: %s (thread waiting)", packet.messageArgs)
                    if( outgoingMessage.networkId == packet.networkId && outgoingMessage.destinationId == packet.sourceId)
                    {
                        logDebug("Found matching response NetworkId:0x%02X=0x%02X sourceId:0x%02X=0x%02X", outgoingMessage.networkId, packet.networkId, outgoingMessage.destinationId, packet.sourceId)
                        outgoingMessage.response_data = packet
                        outgoingMessage.response_data_semaphore.release()
                    }
                    else
                    {
                        logDebug("Not matching expected response NetworkId:0x%02X=0x%02X sourceId:0x%02X=0x%02X", outgoingMessage.networkId, packet.networkId, outgoingMessage.destinationId, packet.sourceId)
                    }
                } else {
                    logDebug("Received UPB_DEVICE_STATE report: %s, no thread waiting, not signaling", packet.messageArgs)
                }
                break
            default:
                logError("Unknown message type: ${messageType}")
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
        setNetworkStatus("Disconnected", e.getMessage())
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
    result = writePimRegister((byte) 0x70, [0x02] as byte[])
    if (result.result) {
        logInfo("PIM was successfully set to Message Mode")
        setModuleStatus("Active")
        setDeviceStatus("ok")
        return true
    } else {
        def error = "Failed to set PIM to Message Mode: ${result.reason}"
        logError(error)
        setModuleStatus("Inactive")
        setDeviceStatus("error", error , true)
        return false
    }
}

def readPimRegister(byte register, int numRegisters) {
    logTrace("readPimRegister(register=0x%02X, numRegisters=%d)", register, numRegisters)

    if (numRegisters < 1 || numRegisters > 16) {
        logError("Invalid number of registers: %d (must be 1-16)", numRegisters)
        return [result: false, reason: "Invalid number of registers: ${numRegisters} (must be 1-16)"]
    }

    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    currentOutgoing.putIfAbsent(device.deviceNetworkId, [:])

    def packet = new ByteArrayOutputStream()
    packet.write([register, numRegisters] as byte[])
    packet.write(checksum(packet.toByteArray()))
    byte[] encodedPacket = HexUtils.byteArrayToHexString(packet.toByteArray()).getBytes()
    logDebug("Encoded PIM read packet: %s", new String(encodedPacket))

    def message = new ByteArrayOutputStream()
    message.write(READ_REGISTER)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytes = message.toByteArray()

    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def outgoing = [
                pim_response: null,
                pim_response_data: null,
                pim_response_semaphore: new Semaphore(0)
        ]
        Map result = [result: false, reason: "Unknown failure"]
        def retry = 0
        loop: while (true) {
            // Reset for retry
            outgoing.pim_response = null
            outgoing.register_data = null
            outgoing.pim_response_semaphore.drainPermits()
            outgoing.register_data_semaphore.drainPermits()
            currentOutgoing.put(device.deviceNetworkId, outgoing)

            // Send message
            sendBytes(bytes)
            logDebug("Read register message sent: %s (register=0x%02X, numRegisters=%d)", new String(encodedPacket), register, numRegisters)

            // Wait for PR/PB/PE
            if (outgoing.pim_response_semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                switch (outgoing.pim_response) {
                    case 'PR':
                        def registerData = outgoing.pim_response_data
                        if (registerData?.register != null && registerData?.values != null) {
                            if (registerData.register == register && registerData.values.length == numRegisters) {
                                logDebug("Received register report (PR) with register=0x%02X, values=%s",
                                        registerData.register, registerData.values)
                                result = [result: true, reason: "Success", data: registerData]
                            } else {
                                logError("PR response invalid: register=0x%02X (expected 0x%02X), values.length=%d (expected %d)",
                                        registerData.register, register, registerData.values.length, numRegisters)
                                result = [result: false, reason: "PR response register or values mismatch"]
                            }
                        } else {
                            logError("Invalid or failed PR response")
                            result = [result: false, reason: "Invalid or failed PR response"]
                        }
                        break loop
                    case 'PB':
                        logWarn("PIM busy (PB), retrying (%d/%d)", retry + 1, maxRetry)
                        break
                    case 'PE':
                        logError("PIM rejected read register message (PE)")
                        result = [result: false, reason: "PIM rejected message (PE)"]
                        break loop
                    default:
                        logWarn("Invalid or no PR/PB/PE response, retrying (%d/%d)", retry + 1, maxRetry)
                        break
                }
            } else {
                logWarn("Timeout waiting for PR/PB/PE, retrying (%d/%d)", retry + 1, maxRetry)
            }

            // Check for max retries
            if (++retry >= maxRetry) {
                logError("Read register 0x%02X aborted: Maximum retries reached", register)
                result = [result: false, reason: "Maximum retries reached"]
                break loop
            }

            // Apply delay for retries
            pauseExecution(100)
        }

        // Set current_outgoing to empty map
        currentOutgoing.put(device.deviceNetworkId, [:])
        logDebug("Set current_outgoing to empty map")
        return result
    }
}

def writePimRegister(byte register, byte[] values) {
    logTrace("writePimRegister(register=%s, values=%s)", register, values)

    if (values.length < 1 || values.length > 16) {
        logError("Invalid number of values: %d (must be 1-16)", values.length)
        return [result: false, reason: "Invalid number of values: ${values.length} (must be 1-16)"]
    }

    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    currentOutgoing.putIfAbsent(device.deviceNetworkId, [:])

    def packet = new ByteArrayOutputStream()
    packet.write(register)
    packet.write(values)
    packet.write(checksum(packet.toByteArray()))
    byte[] encodedPacket = HexUtils.byteArrayToHexString(packet.toByteArray()).getBytes()
    logDebug("Encoded PIM write packet: %s", new String(encodedPacket))

    def message = new ByteArrayOutputStream()
    message.write(WRITE_REGISTER)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytes = message.toByteArray()

    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def outgoing = [
                pim_response: null,
                pim_response_semaphore: new Semaphore(0)
        ]
        Map result = [result: false, reason: "Unknown failure"]
        def retry = 0
        loop: while (true) {
            // Reset for retry
            outgoing.pim_response = null
            outgoing.pim_response_semaphore.drainPermits()
            currentOutgoing.put(device.deviceNetworkId, outgoing)

            // Send message
            sendBytes(bytes)
            logDebug("Write register message sent: %s (register=0x%02X, values=%s)", new String(encodedPacket), register, values)

            // Wait for PA/PB/PE
            if (outgoing.pim_response_semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                switch (outgoing.pim_response) {
                    case 'PA':
                        logDebug("PIM accepted write register message (PA)")
                        result = [result: true, reason: "Success"]
                        break loop
                    case 'PB':
                        logWarn("PIM busy (PB), retrying (%d/%d)", retry + 1, maxRetry)
                        break
                    case 'PE':
                        logError("PIM rejected write register message (PE)")
                        result = [result: false, reason: "PIM rejected message (PE)"]
                        break loop
                    default:
                        logWarn("Invalid or no PA/PB/PE response, retrying (%d/%d)", retry + 1, maxRetry)
                        break
                }
            } else {
                logWarn("Timeout waiting for PA/PB/PE, retrying (%d/%d)", retry + 1, maxRetry)
            }

            // Check for max retries
            if (++retry >= maxRetry) {
                logError("Write register 0x%02X aborted: Maximum retries reached", register)
                result = [result: false, reason: "Maximum retries reached"]
                break loop
            }

            // Apply delay for retries
            pauseExecution(100)
        }

        // Set current_outgoing to empty map
        currentOutgoing.put(device.deviceNetworkId, [:])
        logDebug("Set current_outgoing to empty map")
        return result
    }
}

def transmitMessage(short controlWord, byte networkId, byte destinationId, byte sourceId, byte messageDataId, byte[] messageArgument) {
    logTrace("transmitMessage(controlWord=0x%04X, networkId=0x%02X, destinationId=0x%02X, sourceId=0x%02X, messageDataId=0x%02X, messageArgument=%s)",
            controlWord, networkId & 0xFF, destinationId & 0xFF, sourceId & 0xFF, messageDataId & 0xFF, messageArgument ? HexUtils.byteArrayToHexString(messageArgument) : "null")
    logDebug("ACK flags: pulse=${(controlWord & ACKRQ_PULSE) != 0}, msg=${(controlWord & ACKRQ_MSG) != 0}, id=${(controlWord & ACKRQ_ID) != 0}")

    deviceMutexes.putIfAbsent(device.deviceNetworkId, new Object())
    currentOutgoing.putIfAbsent(device.deviceNetworkId, [:])

    byte[] packet = buildPacket(controlWord, networkId, destinationId, sourceId, messageDataId, messageArgument)
    byte[] encodedPacket = HexUtils.byteArrayToHexString(packet).getBytes()
    logDebug("Encoded PIM write packet: %s", new String(encodedPacket))

    def message = new ByteArrayOutputStream()
    message.write(TRANSMIT_MESSAGE)
    message.write(encodedPacket)
    message.write(EOM)
    byte[] bytesToSend = message.toByteArray()

    // Main gatekeeper lock to ensure one command transaction at a time.
    synchronized (deviceMutexes.get(device.deviceNetworkId)) {
        def expectsPU = (messageDataId == UPB_REPORT_STATE)
        def expectsAck = (controlWord & ACKRQ_PULSE) != 0

        def outgoing = [
                pim_response: null,
                pim_response_semaphore: new Semaphore(0),
                pim_ack: null,
                pim_ack_semaphore: new Semaphore(0),
                controlWord: controlWord
        ]

        if (expectsPU) {
            outgoing.response_data = null
            outgoing.response_data_semaphore = new Semaphore(0)
            outgoing.networkId = networkId
            outgoing.destinationId = destinationId
        }

        Map result = [result: false, reason: "Transaction failed after all retries."]
        int retry = 0

        try {
            loop: while (retry < maxRetry) {
                // Reset state for this attempt
                outgoing.pim_response = null
                outgoing.pim_ack = null
                outgoing.pim_response_semaphore.drainPermits()
                outgoing.pim_ack_semaphore.drainPermits()
                if (expectsPU) {
                    outgoing.response_data = null
                    outgoing.response_data_semaphore?.drainPermits()
                }
                currentOutgoing.put(device.deviceNetworkId, outgoing)

                // ==========================================================
                // STAGE A: Send and Wait for PIM Acceptance (PA/PB/PE)
                // ==========================================================
                sendBytes(bytesToSend)
                logDebug("UPB message sent to PIM (Attempt ${retry + 1}/${maxRetry})")

                if (!outgoing.pim_response_semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                    logWarn("Timeout waiting for PA/PB/PE. Retrying...")
                    retry++
                    pauseExecution(100)
                    continue loop
                }

                // Process the PIM's initial response
                switch (outgoing.pim_response) {
                    case 'PA':
                        logDebug("PIM accepted UPB message (PA).")
                        break // Success, break from switch to proceed to next stage.
                    case 'PB':
                        logWarn("PIM busy (PB). Retrying...")
                        retry++
                        pauseExecution(100)
                        continue loop // Go to top of while loop for next attempt.
                    case 'PE':
                        logError("PIM rejected UPB message (PE).")
                        result = [result: false, reason: "PIM rejected message (PE)"]
                        break loop // Terminal failure, abort transaction.
                    default:
                        logError("Invalid or unexpected PIM response: '${outgoing.pim_response}'.")
                        result = [result: false, reason: "Invalid PIM response: ${outgoing.pim_response}"]
                        break loop // Terminal failure, abort transaction.
                }

                // ==========================================================
                // STAGE B: Wait for Transmission Acknowledgment (PK/PN)
                // ==========================================================
                if (!outgoing.pim_ack_semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                    logError("Timeout waiting for PK/PN from remote UPB device.")
                    result = [result: false, reason: "Timeout: No ACK/NAK from remote UPB device."]
                    break loop
                }

                // Check for ACK/NAK mismatch errors
                if (outgoing.pim_ack == 'PK' && !expectsAck) {
                    logError("Unexpected PK received, but ACKRQ_PULSE was not set.")
                    result = [result: false, reason: "Unexpected PK received"]
                    break loop
                }
                if (outgoing.pim_ack == 'PN' && expectsAck) {
                    logError("Transmission failed with NAK (PN), but an ACK pulse was expected.")
                    result = [result: false, reason: "NoAck: Device did not acknowledge message (PN)"]
                    break loop
                }

                logDebug("Transmission completed successfully with ${outgoing.pim_ack}.")

                // ==========================================================
                // STAGE C: Wait for Data Payload (if expected)
                // ==========================================================
                if (expectsPU) {
                    if (!outgoing.response_data_semaphore.tryAcquire(maxProcessingTime, TimeUnit.MILLISECONDS)) {
                        logError("Timeout waiting for data response (PU) from remote device.")
                        result = [result: false, reason: "Timeout: No data response from remote UPB device."]
                        break loop
                    }

                    def response = outgoing.response_data
                    if (response && response.networkId == outgoing.networkId &&
                            response.sourceId == outgoing.destinationId &&
                            response.messageDataId == UPB_DEVICE_STATE) {

                        logDebug("Received valid UPB_DEVICE_STATE report.")
                        result = [result: true, reason: "Success", data: response.messageArgs]
                    } else {
                        logError("Received invalid or mismatched data response (PU). Full response: ${response}")
                        result = [result: false, reason: "Invalid data response (PU)"]
                    }
                } else {
                    // No data was expected, so the transaction is successful.
                    result = [result: true, reason: "Success"]
                }

                // The entire transaction is complete. Exit the loop.
                break loop
            }

            // If the loop finishes due to max retries, update the reason.
            if (!result.result && retry >= maxRetry) {
                result.reason = "Transaction failed: Maximum retries reached."
            }

        } finally {
            // Always clear the state map to prevent stale data from affecting the next command.
            currentOutgoing.put(device.deviceNetworkId, [:])
            logDebug("Transaction finished. Cleared outgoing state.")
        }

        return result
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