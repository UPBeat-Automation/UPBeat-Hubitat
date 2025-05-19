/*
* Hubitat Driver: UPB Non-Dimming Switch
* Description: Universal Powerline Bus Non-Dimming Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

metadata {
    definition(name: "UPB Non-Dimming Switch", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "") {
        capability "Switch"
        capability "Refresh"
        attribute "status", "enum", ["ok", "error"]
    }

    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        input name: "networkId", type: "number", title: "Network ID", required: true, range: "0..255"
        input name: "deviceId", type: "number", title: "Device ID", required: true, range: "0..255"
        input name: "channelId", type: "number", title: "Channel ID", required: true, range: "0..255"
        input name: "receiveComponent1", type: "text", title: "Receive Component 1"
        input name: "receiveComponent2", type: "text", title: "Receive Component 2"
        input name: "receiveComponent3", type: "text", title: "Receive Component 3"
        input name: "receiveComponent4", type: "text", title: "Receive Component 4"
        input name: "receiveComponent5", type: "text", title: "Receive Component 5"
        input name: "receiveComponent6", type: "text", title: "Receive Component 6"
        input name: "receiveComponent7", type: "text", title: "Receive Component 7"
        input name: "receiveComponent8", type: "text", title: "Receive Component 8"
        input name: "receiveComponent9", type: "text", title: "Receive Component 9"
        input name: "receiveComponent10", type: "text", title: "Receive Component 10"
        input name: "receiveComponent11", type: "text", title: "Receive Component 11"
        input name: "receiveComponent12", type: "text", title: "Receive Component 12"
        input name: "receiveComponent13", type: "text", title: "Receive Component 13"
        input name: "receiveComponent14", type: "text", title: "Receive Component 14"
        input name: "receiveComponent15", type: "text", title: "Receive Component 15"
        input name: "receiveComponent16", type: "text", title: "Receive Component 16"
    }
}

/***************************************************************************
 * Helper Functions
 ***************************************************************************/
private void isCorrectParent() {
    def parentApp = getParent()
    if (!parentApp || parentApp.name != "UPBeat App") {
        throw new IllegalStateException("${device.name ?: 'Device'} must be created by the UPBeat App. Manual creation is not supported.")
    }
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed() {
    logTrace "installed()"
    try {
        isCorrectParent()
        logDebug "Installing UPB Non-Dimming Switch"
        // Initialize state.receiveComponents to ensure it's not null
        device.updateDataValue("receiveComponents", JsonOutput.toJson([:]))
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def updated() {
    logTrace "updated()"
    try {
        isCorrectParent()
        // Process UPB receive link slots
        def components = [:] // Maps Link ID to action (level, rate, slot)
        def errors = []

        try {
            // Process each of the 16 slots
            (1..16).each { slot ->
                def slotInput = settings."receiveComponent${slot}"?.trim()

                if (slotInput) {
                    // Split the input into parts (linkID:level:rate or linkID:level)
                    def parts = slotInput.split(":")
                    if (parts.size() < 2 || parts.size() > 3) {
                        errors << "Slot ${slot}: Invalid format: ${slotInput}. Expected linkID:level:rate (rate optional). linkID: 1-250, level: 0-100, rate: 0-255."
                        return
                    }

                    def linkId, level, rate
                    try {
                        linkId = parts[0].toInteger()
                        level = parts[1].toInteger()
                        rate = parts.size() == 3 ? parts[2].toInteger() : 0 // Default rate to 0 if not specified
                    } catch (NumberFormatException e) {
                        errors << "Slot ${slot}: Invalid number format in ${slotInput}: ${e.message}. Ensure linkID, level, and rate are valid integers."
                        return
                    }

                    // Validate linkId (1-250)
                    if (linkId < 1 || linkId > 250) {
                        errors << "Slot ${slot}: Invalid linkId in ${slotInput}: ${linkId}. Must be between 1 and 250."
                        return
                    }

                    // Validate level (0-100)
                    if (level < 0 || level > 100) {
                        errors << "Slot ${slot}: Invalid level in ${slotInput}: ${level}. Must be between 0 and 100."
                        return
                    }

                    // Validate rate (0-255)
                    if (rate < 0 || rate > 255) {
                        errors << "Slot ${slot}: Invalid rate in ${slotInput}: ${rate}. Must be between 0 and 255."
                        return
                    }

                    // Use the linkId as the key (no padding)
                    def linkIdKey = linkId.toString()

                    // Check for duplicate Link IDs across slots
                    if (components.containsKey(linkIdKey)) {
                        errors << "Slot ${slot}: Duplicate Link ID ${linkId} already defined in slot ${components[linkIdKey].slot}."
                        return
                    }

                    // Store the component for lookup
                    components[linkIdKey] = [
                            level: level,
                            rate: rate,
                            slot: slot.toString() // Store the slot number for reference
                    ]
                }
            }

            // If there are any errors, log them and throw an exception
            if (errors) {
                errors.each { error -> logWarn error }
                throw new Exception("Invalid receive link configuration. Check the logs for details.")
            }
            // Store the components in data (serialized as JSON)
            device.updateDataValue("receiveComponents", JsonOutput.toJson(components))
            // Log the configuration in a table-like format
            def logMessage = "Stored UPB receive links for ${device.deviceNetworkId}:\n"
            (1..16).each { slot ->
                def linkId = components.find { it.value.slot == slot.toString() }?.key?.toInteger()
                if (linkId) {
                    def comp = components[linkId.toString()]
                    logMessage += "Slot ${slot}: Link ID ${linkId}, Level ${comp.level}%, Rate ${comp.rate}\n"
                } else {
                    logMessage += "Slot ${slot}: Unused\n"
                }
            }
            logDebug logMessage
        } catch (Exception e) {
            logWarn "Failed to parse receive links: ${e.message}. Please check the format (linkID:level:rate, rate optional)."
            throw new Exception("Failed to update receive links: ${e.message}. Check the logs for details.")
        }
        // Call updateDeviceSettings directly on the parent app
        def result = getParent().updateDeviceSettings(device, settings)
        if (result.success) {
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            log.error "Failed to update device: ${result.error}"
            sendEvent(name: "status", value: "error", descriptionText: result.error, isStateChange: true)
            return
        }
        // If switch driver is changed, clear the states
        state.clear()
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def parse(String description) {
    logTrace "parse(${description})"
    try {
        isCorrectParent()
        // Currently a no-op, but adding parent check for consistency
        logDebug "Parse called with description: ${description}"
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

/***************************************************************************
 * Handlers for Driver Data
 ***************************************************************************/
def updateNetworkId(Long networkId) {
    logTrace "updateNetworkId()"
    try {
        isCorrectParent()
        device.updateSetting("networkId", [type: "number", value: networkId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def updateDeviceId(Long deviceId) {
    logTrace "updateDeviceId()"
    try {
        isCorrectParent()
        device.updateSetting("deviceId", [type: "number", value: deviceId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def updateChannelId(Long channelId) {
    logTrace "updateChannelId()"
    try {
        isCorrectParent()
        device.updateSetting("channelId", [type: "number", value: channelId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

/***************************************************************************
 * Handlers for Driver Capabilities
 ***************************************************************************/
def refresh() {
    logDebug "refresh()"
    try {
        isCorrectParent()
        byte[] data = getParent().buildDeviceStateRequestCommand(settings.networkId.intValue(), settings.deviceId.intValue())
        logDebug "UPB Device State Request Command [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Device State Request successfully sent [${data}]"
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue Device State Request command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    } catch (Exception e) {
        logWarn "Call to refresh failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Refresh failed: ${e.message}", isStateChange: true)
    }
}

def flash(BigDecimal rateToFlash) {
    logDebug "Flash Rate [${rateToFlash}]"
    try {
        isCorrectParent()
        byte[] data = getParent().blinkCommand(settings.networkId.intValue(), settings.deviceId.intValue(), rateToFlash, settings.channelId.intValue())
        logDebug "UPB Flash Command[${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "UPB Flash [${data}]"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue flash command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def on() {
    logDebug "Sending ON to device [${settings.deviceId}]"
    try {
        isCorrectParent()
        logDebug "Sending ON to device [${settings.deviceId}]"
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 100, 0, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue on command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    } catch (Exception e) {
        logWarn "Call to on failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "On command failed: ${e.message}", isStateChange: true)
    }
}

def off() {
    logDebug "Sending OFF to device [${settings.deviceId}]"
    try {
        isCorrectParent()
        logDebug "Sending OFF to device [${settings.deviceId}]"
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 0, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "off", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue off command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    } catch (Exception e) {
        logWarn "Call to off failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Off command failed: ${e.message}", isStateChange: true)
    }
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logDebug "Received UPB scene command for ${device.deviceNetworkId}: Link ID ${linkId}"
    try {
        isCorrectParent()
        // Retrieve and deserialize the receiveComponents map from data
        def receiveComponents = [:]
        def jsonData = device.getDataValue("receiveComponents")
        if (jsonData) {
            receiveComponents = new JsonSlurper().parseText(jsonData)
        }

        // Use the linkId as the key (no padding)
        def linkIdKey = linkId.toString()
        def component = receiveComponents?.get(linkIdKey)

        if (component) {
            def level = component.level
            def rate = component.rate
            def slot = component.slot
            logDebug "Executing action for Link ID ${linkId} (Slot ${slot}) on ${device.deviceNetworkId}: Level=${level}, Rate=${rate}"
            if (level == 0) {
                sendEvent(name: "switch", value: "off")
            } else {
                sendEvent(name: "switch", value: "on")
            }
            sendEvent(name: "lastReceivedLinkId", value: linkId)
            logDebug "Rate ${rate} not fully implemented in Hubitat; action applied instantly"
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug "No action defined for Link ID ${linkId} on ${device.deviceNetworkId}. Check the receive link configuration."
            sendEvent(name: "lastReceivedLinkId", value: linkId)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}

def handleDeviceEvent(int level, int networkId, int sourceId, int destinationId, List args) {
    logTrace "handleDeviceState(level=${level}, networkId=${networkId}, sourceId=${sourceId}, destinationId=${destinationId}, args=${args})"
    try {
        isCorrectParent()
        if (settings.networkId != networkId || settings.deviceId != destinationId) {
            logDebug "Ignoring deviceState for Network ID ${networkId} (expected ${settings.networkId}), Device ID ${destinationId} (expected ${settings.deviceId})"
            return
        }
        def switchValue = (level == 0) ? "off" : "on"
        logDebug "Updating switch to ${switchValue} for device [${settings.deviceId}]"
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    }
}