/*
* Hubitat Driver: UPB Non-Dimming Switch
* Description: Universal Powerline Bus Non-Dimming Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib
#include UPBeat.UPBeatDriverLib
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
        input name: "networkId", type: "number", title: "Network ID", description: "UPB Network ID (0-255)", required: true, range: "0..255"
        input name: "deviceId", type: "number", title: "Device ID", description: "UPB Device ID (1-250)", required: true, range: "1..250"
        input name: "channelId", type: "number", title: "Channel ID", description: "UPB Channel ID (1-255)", required: true, range: "1..255"
        input name: "receiveComponent1", type: "text", title: "Receive Component 1", description: "Format: linkId:level"
        input name: "receiveComponent2", type: "text", title: "Receive Component 2", description: "Format: linkId:level"
        input name: "receiveComponent3", type: "text", title: "Receive Component 3", description: "Format: linkId:level"
        input name: "receiveComponent4", type: "text", title: "Receive Component 4", description: "Format: linkId:level"
        input name: "receiveComponent5", type: "text", title: "Receive Component 5", description: "Format: linkId:level"
        input name: "receiveComponent6", type: "text", title: "Receive Component 6", description: "Format: linkId:level"
        input name: "receiveComponent7", type: "text", title: "Receive Component 7", description: "Format: linkId:level"
        input name: "receiveComponent8", type: "text", title: "Receive Component 8", description: "Format: linkId:level"
        input name: "receiveComponent9", type: "text", title: "Receive Component 9", description: "Format: linkId:level"
        input name: "receiveComponent10", type: "text", title: "Receive Component 10", description: "Format: linkId:level"
        input name: "receiveComponent11", type: "text", title: "Receive Component 11", description: "Format: linkId:level"
        input name: "receiveComponent12", type: "text", title: "Receive Component 12", description: "Format: linkId:level"
        input name: "receiveComponent13", type: "text", title: "Receive Component 13", description: "Format: linkId:level"
        input name: "receiveComponent14", type: "text", title: "Receive Component 14", description: "Format: linkId:level"
        input name: "receiveComponent15", type: "text", title: "Receive Component 15", description: "Format: linkId:level"
        input name: "receiveComponent16", type: "text", title: "Receive Component 16", description: "Format: linkId:level"
    }
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed() {
    logTrace("installed()")
    try {
        isCorrectParent()
        logDebug("Installing UPB Non-Dimming Switch")
        // Initialize state.receiveComponents to ensure it's not null
        device.updateDataValue("receiveComponents", JsonOutput.toJson([:]))
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updated() {
    logTrace("updated()")
    try {
        isCorrectParent()

        // Update parent device settings this will updated the device ID
        def result = parent.updateDeviceSettings(device, settings)
        if (result.success) {
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logError("Failed to update device: ${result.error}")
            sendEvent(name: "status", value: "error", descriptionText: result.error, isStateChange: true)
            return
        }

        // Get validated receiveComponents
        def components = getReceiveComponents()

        device.updateDataValue("receiveComponents", JsonOutput.toJson(components))

        // Clear state
        state.clear()
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn("Failed to update: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * Handlers for Driver Data
 ***************************************************************************/
def updateNetworkId(Long networkId) {
    logTrace("updateNetworkId(${networkId})")
    try {
        isCorrectParent()
        device.updateSetting("networkId", [type: "number", value: networkId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updateDeviceId(Long deviceId) {
    logTrace("updateDeviceId(${deviceId})")
    try {
        isCorrectParent()
        device.updateSetting("deviceId", [type: "number", value: deviceId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updateChannelId(Long channelId) {
    logTrace("updateChannelId(${channelId})")
    try {
        isCorrectParent()
        device.updateSetting("channelId", [type: "number", value: channelId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updateReceiveComponentSlot(int slot, int linkId, int level) {
    logTrace("updateReceiveComponentSlot(${slot})")
    try {
        isCorrectParent()
        device.updateSetting("receiveComponent${slot}", [type: "string", value: "${linkId}:${level}"])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * Handlers for Driver Capabilities
 ***************************************************************************/
def refresh() {
    logTrace("refresh()")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    // Validate inputs
    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("Network ID ${settings.networkId} is invalid or out of range (0-255)")
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("Device ID ${settings.deviceId} is invalid or out of range (1-250)")
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }

    def result = getParent().requestDeviceState(settings.networkId.intValue(), settings.deviceId.intValue(), 0)
    if (result.result) {
        logDebug("Device state request succeeded")
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("Device state request failed: %s", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    return result
}

def on() {
    logTrace("on()")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    // Validate inputs
    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("Network ID ${settings.networkId} is invalid or out of range (0-255)")
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("Device ID ${settings.deviceId} is invalid or out of range (1-250)")
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }
    if (!settings.channelId || settings.channelId < 1 || settings.channelId > 255) {
        logError("Channel ID ${settings.channelId} is invalid or out of range (1-255)")
        sendEvent(name: "status", value: "error", descriptionText: "Channel ID must be 1-255", isStateChange: true)
        return [result: false, reason: "Channel ID must be 1-255"]
    }

    def result = getParent().gotoLevel(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 100, 0, settings.channelId.intValue())
    if (result.result) {
        logDebug("On command succeeded")
        sendEvent(name: "switch", value: "on", isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("On command failed: %s", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    return result
}

def off() {
    logTrace("off()")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    // Validate inputs
    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("Network ID ${settings.networkId} is invalid or out of range (0-255)")
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("Device ID ${settings.deviceId} is invalid or out of range (1-250)")
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }
    if (!settings.channelId || settings.channelId < 1 || settings.channelId > 255) {
        logError("Channel ID ${settings.channelId} is invalid or out of range (1-255)")
        sendEvent(name: "status", value: "error", descriptionText: "Channel ID must be 1-255", isStateChange: true)
        return [result: false, reason: "Channel ID must be 1-255"]
    }

    def result = getParent().gotoLevel(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 0, 0, settings.channelId.intValue())
    if (result.result) {
        logDebug("Off command succeeded")
        sendEvent(name: "switch", value: "off", isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("Off command failed: %s", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    return result
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace("handleLinkEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})")
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
            def slot = component.slot
            switch(eventType){
                case "UPB_ACTIVATE_LINK":
                    sendEvent(name: "switch", value: (level == 0) ? "off" : "on")
                    break
                case "UPB_DEACTIVATE_LINK":
                    sendEvent(name: "switch", value: "off")
                    break
                default:
                    sendEvent(name: "status", value: "error", descriptionText: "Unknown event type eventType=${eventType}", isStateChange: true)
                    return
                    break
            }
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug("No action defined for Link ID ${linkId} on ${device.deviceNetworkId}. Check the receive link configuration.")
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def handleGotoEvent(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int level, int rate, int channel)
{
    logTrace("handleGotoEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, destinationId=${destinationId}, level=${level}, rate=${rate}, channel=${channel})")
    try {
        isCorrectParent()
        def switchValue = (level == 0) ? "off" : "on"
        logDebug("Updating switch to ${switchValue} for device [${settings.deviceId}]")
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def handleDeviceStateReport(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int[] messageArgs) {
    logTrace("handleDeviceEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, destinationId=${destinationId}, args=${messageArgs})")
    try {
        isCorrectParent()
        int channel = settings.channelId.intValue() - 1
        int level = messageArgs.size() > channel ? Math.min(messageArgs[channel], 100) : 0

        def switchValue = (level == 0) ? "off" : "on"
        logDebug("Updating switch to ${switchValue} for device [${settings.deviceId}]")
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}