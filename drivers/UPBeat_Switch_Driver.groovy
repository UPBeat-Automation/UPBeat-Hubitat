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

def getReceiveComponents() {
    def components = [:]
    def hasErrors = false
    def isDimmable = device.hasCapability("SwitchLevel")
    (1..16).each { slot ->
        def slotInput = settings."receiveComponent${slot}"?.trim()
        if (slotInput) {
            def parts = slotInput.split(":")
            if (parts.size() < 2 || parts.size() > 3) {
                logWarn "Invalid format in receiveComponent${slot}: ${slotInput}. Expected linkID:level, setting value removed"
                device.updateSetting("receiveComponent${slot}", "")
                hasErrors = true
                return
            }
            try {
                def linkId = parts[0].toInteger()
                def level = parts[1].toInteger()
                if (linkId < 1 || linkId > 250) {
                    logWarn "Invalid linkId in receiveComponent${slot}: ${linkId}. Must be 1-250, setting value removed"
                    device.updateSetting("receiveComponent${slot}", "")
                    hasErrors = true
                    return
                }
                if (isDimmable) {
                    if (level < 0 || level > 100) {
                        logWarn "Invalid level in receiveComponent${slot}: ${level}. Must be 0-100 for dimmable device, setting value removed"
                        device.updateSetting("receiveComponent${slot}", "")
                        hasErrors = true
                        return
                    }
                } else {
                    if (level != 0 && level != 100) {
                        logWarn "Invalid level in receiveComponent${slot}: ${level}. Must be 0 or 100 for non-dimmable device, setting value removed"
                        device.updateSetting("receiveComponent${slot}", "")
                        hasErrors = true
                        return
                    }
                }
                def linkIdKey = linkId.toString()
                if (components.containsKey(linkIdKey)) {
                    logWarn "Duplicate linkId ${linkId} in receiveComponent${slot}, setting value removed"
                    device.updateSetting("receiveComponent${slot}", "")
                    hasErrors = true
                    return
                }
                components[linkIdKey] = [level: level]
            } catch (NumberFormatException e) {
                logWarn "Invalid number format in receiveComponent${slot}: ${slotInput}, setting value removed"
                device.updateSetting("receiveComponent${slot}", "")
                hasErrors = true
            } catch (Exception e) {
                logWarn "Unexpected error in receiveComponent${slot}: ${e.message}, setting value removed"
                device.updateSetting("receiveComponent${slot}", "")
                hasErrors = true
            }
        }
    }
    if (hasErrors) {
        sendEvent(name: "status", value: "error", descriptionText: "Invalid receiveComponents detected; check logs and verify settings", isStateChange: true)
    } else if (device.currentValue("status") != "ok") {
        sendEvent(name: "status", value: "ok", descriptionText: "All receiveComponents valid", isStateChange: true)
    }
    return components
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

        // Update parent device settings this will updated the device ID
        def result = parent.updateDeviceSettings(device, settings)
        if (result.success) {
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logError "Failed to update device: ${result.error}"
            sendEvent(name: "status", value: "error", descriptionText: result.error, isStateChange: true)
            return
        }

        // Get validated receiveComponents
        def components = getReceiveComponents()

        device.updateDataValue("receiveComponents", JsonOutput.toJson(components))

        // Clear state
        state.clear()
    } catch (IllegalStateException e) {
        logError e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn "Failed to update: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
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