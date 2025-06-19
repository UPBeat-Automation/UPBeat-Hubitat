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
    logTrace("[${device.deviceNetworkId}] installed: Entering.")
    try {
        isCorrectParent()
        logInfo("[${device.deviceNetworkId}] installed: Installing UPB Non-Dimming Switch.")
        device.updateDataValue("receiveComponents", JsonOutput.toJson([:]))
        sendEvent(name: "status", value: "ok", isStateChange: false)
        logInfo("[${device.deviceNetworkId}] installed: Driver installed successfully.")
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] installed: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] installed: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] installed: Exiting.")
}

def updated() {
    logTrace("[${device.deviceNetworkId}] updated: Entering.")
    try {
        isCorrectParent()
        logDebug("[${device.deviceNetworkId}] updated: Validating settings: networkId=%d, deviceId=%d, channelId=%d.",
                settings.networkId, settings.deviceId, settings.channelId)

        def result = parent.updateDeviceSettings(device, settings)
        if (result.success) {
            logInfo("[${device.deviceNetworkId}] updated: Device settings updated successfully.")
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logError("[${device.deviceNetworkId}] updated: Failed to update device settings: %s.", result.error)
            sendEvent(name: "status", value: "error", descriptionText: result.error, isStateChange: true)
            return
        }

        def components = getReceiveComponents()
        logDebug("[${device.deviceNetworkId}] updated: Parsed receive components: %s.", components)
        device.updateDataValue("receiveComponents", JsonOutput.toJson(components))

        state.clear()
        logInfo("[${device.deviceNetworkId}] updated: Cleared state.")
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] updated: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn("[${device.deviceNetworkId}] updated: Failed to update: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] updated: Exiting.")
}

/***************************************************************************
 * Handlers for Driver Data
 ***************************************************************************/
def updateNetworkId(Long networkId) {
    logTrace("[${device.deviceNetworkId}] updateNetworkId: Entering with networkId=%d.", networkId)
    try {
        isCorrectParent()
        logInfo("[${device.deviceNetworkId}] updateNetworkId: Updating network ID to %d.", networkId)
        device.updateSetting("networkId", [type: "number", value: networkId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] updateNetworkId: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] updateNetworkId: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] updateNetworkId: Exiting.")
}

def updateDeviceId(Long deviceId) {
    logTrace("[${device.deviceNetworkId}] updateDeviceId: Entering with deviceId=%d.", deviceId)
    try {
        isCorrectParent()
        logInfo("[${device.deviceNetworkId}] updateDeviceId: Updating device ID to %d.", deviceId)
        device.updateSetting("deviceId", [type: "number", value: deviceId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] updateDeviceId: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] updateDeviceId: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] updateDeviceId: Exiting.")
}

def updateChannelId(Long channelId) {
    logTrace("[${device.deviceNetworkId}] updateChannelId: Entering with channelId=%d.", channelId)
    try {
        isCorrectParent()
        logInfo("[${device.deviceNetworkId}] updateChannelId: Updating channel ID to %d.", channelId)
        device.updateSetting("channelId", [type: "number", value: channelId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] updateChannelId: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] updateChannelId: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] updateChannelId: Exiting.")
}

def updateReceiveComponentSlot(int slot, int linkId, int level) {
    logTrace("[${device.deviceNetworkId}] updateReceiveComponentSlot: Entering with slot=%d, linkId=%d, level=%d.", slot, linkId, level)
    try {
        isCorrectParent()
        logInfo("[${device.deviceNetworkId}] updateReceiveComponentSlot: Updating slot %d to linkId=%d, level=%d.", slot, linkId, level)
        device.updateSetting("receiveComponent${slot}", [type: "string", value: "${linkId}:${level}"])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] updateReceiveComponentSlot: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] updateReceiveComponentSlot: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] updateReceiveComponentSlot: Exiting.")
}

/***************************************************************************
 * Handlers for Driver Capabilities
 ***************************************************************************/
def refresh() {
    logTrace("[${device.deviceNetworkId}] refresh: Entering.")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] refresh: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("[${device.deviceNetworkId}] refresh: Invalid network ID: %d (must be 0-255).", settings.networkId)
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("[${device.deviceNetworkId}] refresh: Invalid device ID: %d (must be 1-250).", settings.deviceId)
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }

    logDebug("[${device.deviceNetworkId}] refresh: Requesting device state for networkId=0x%02X, deviceId=0x%02X.", settings.networkId, settings.deviceId)
    def result = getParent().requestDeviceState(settings.networkId.intValue(), settings.deviceId.intValue(), 0)
    if (result.result) {
        logInfo("[${device.deviceNetworkId}] refresh: Device state request succeeded.")
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("[${device.deviceNetworkId}] refresh: Device state request failed: %s.", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] refresh: Exiting with result=%s.", result)
    return result
}

def on() {
    logTrace("[${device.deviceNetworkId}] on: Entering.")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] on: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("[${device.deviceNetworkId}] on: Invalid network ID: %d (must be 0-255).", settings.networkId)
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("[${device.deviceNetworkId}] on: Invalid device ID: %d (must be 1-250).", settings.deviceId)
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }
    if (!settings.channelId || settings.channelId < 1 || settings.channelId > 255) {
        logError("[${device.deviceNetworkId}] on: Invalid channel ID: %d (must be 1-255).", settings.channelId)
        sendEvent(name: "status", value: "error", descriptionText: "Channel ID must be 1-255", isStateChange: true)
        return [result: false, reason: "Channel ID must be 1-255"]
    }

    logDebug("[${device.deviceNetworkId}] on: Sending ON to networkId=0x%02X, deviceId=0x%02X, channelId=0x%02X.", settings.networkId, settings.deviceId, settings.channelId)
    def result = getParent().gotoLevel(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 100, 0, settings.channelId.intValue())
    if (result.result) {
        logInfo("[${device.deviceNetworkId}] on: Command succeeded: switch=on.")
        sendEvent(name: "switch", value: "on", isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("[${device.deviceNetworkId}] on: Command failed: %s.", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] on: Exiting with result=%s.", result)
    return result
}

def off() {
    logTrace("[${device.deviceNetworkId}] off: Entering.")
    try {
        isCorrectParent()
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] off: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return [result: false, reason: e.message]
    }

    if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
        logError("[${device.deviceNetworkId}] off: Invalid network ID: %d (must be 0-255).", settings.networkId)
        sendEvent(name: "status", value: "error", descriptionText: "Network ID must be 0-255", isStateChange: true)
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (!settings.deviceId || settings.deviceId < 1 || settings.deviceId > 250) {
        logError("[${device.deviceNetworkId}] off: Invalid device ID: %d (must be 1-250).", settings.deviceId)
        sendEvent(name: "status", value: "error", descriptionText: "Device ID must be 1-250", isStateChange: true)
        return [result: false, reason: "Device ID must be 1-250"]
    }
    if (!settings.channelId || settings.channelId < 1 || settings.channelId > 255) {
        logError("[${device.deviceNetworkId}] off: Invalid channel ID: %d (must be 1-255).", settings.channelId)
        sendEvent(name: "status", value: "error", descriptionText: "Channel ID must be 1-255", isStateChange: true)
        return [result: false, reason: "Channel ID must be 1-255"]
    }

    logDebug("[${device.deviceNetworkId}] off: Sending OFF to networkId=0x%02X, deviceId=0x%02X, channelId=0x%02X.", settings.networkId, settings.deviceId, settings.channelId)
    def result = getParent().gotoLevel(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 0, 0, settings.channelId.intValue())
    if (result.result) {
        logInfo("[${device.deviceNetworkId}] off: Command succeeded: switch=off.")
        sendEvent(name: "switch", value: "off", isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } else {
        logError("[${device.deviceNetworkId}] off: Command failed: %s.", result.reason)
        sendEvent(name: "status", value: "error", descriptionText: result.reason, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] off: Exiting with result=%s.", result)
    return result
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace("[${device.deviceNetworkId}] handleLinkEvent: Entering with eventSource=%s, eventType=%s, networkId=0x%02X, sourceId=0x%02X, linkId=%d.",
            eventSource, eventType, networkId, sourceId, linkId)
    try {
        isCorrectParent()
        def receiveComponents = [:]
        def jsonData = device.getDataValue("receiveComponents")
        if (jsonData) {
            logTrace("[${device.deviceNetworkId}] handleLinkEvent: Parsing receive components: %s.", jsonData)
            receiveComponents = new JsonSlurper().parseText(jsonData)
        }

        def linkIdKey = linkId.toString()
        def component = receiveComponents?.get(linkIdKey)
        if (component) {
            def level = component.level
            def slot = component.slot
            logDebug("[${device.deviceNetworkId}] handleLinkEvent: Found component for linkId=%d: level=%d, slot=%d.", linkId, level, slot)
            switch (eventType) {
                case "UPB_ACTIVATE_LINK":
                    logInfo("[${device.deviceNetworkId}] handleLinkEvent: Processing UPB_ACTIVATE_LINK, setting switch=%s.", level == 0 ? "off" : "on")
                    sendEvent(name: "switch", value: level == 0 ? "off" : "on", isStateChange: true)
                    break
                case "UPB_DEACTIVATE_LINK":
                    logInfo("[${device.deviceNetworkId}] handleLinkEvent: Processing UPB_DEACTIVATE_LINK, setting switch=off.")
                    sendEvent(name: "switch", value: "off", isStateChange: true)
                    break
                default:
                    logError("[${device.deviceNetworkId}] handleLinkEvent: Unknown event type: %s.", eventType)
                    sendEvent(name: "status", value: "error", descriptionText: "Unknown event type: ${eventType}", isStateChange: true)
                    return
            }
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug("[${device.deviceNetworkId}] handleLinkEvent: No action defined for linkId=%d.", linkId)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] handleLinkEvent: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] handleLinkEvent: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] handleLinkEvent: Exiting.")
}

def handleGotoEvent(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int level, int rate, int channel) {
    logTrace("[${device.deviceNetworkId}] handleGotoEvent: Entering with eventSource=%s, eventType=%s, networkId=0x%02X, sourceId=0x%02X, destinationId=0x%02X, level=%d, rate=%d, channel=0x%02X.",
            eventSource, eventType, networkId, sourceId, destinationId, level, rate, channel)
    try {
        isCorrectParent()
        def switchValue = (level == 0) ? "off" : "on"
        logInfo("[${device.deviceNetworkId}] handleGotoEvent: Updating switch=%s for deviceId=0x%02X.", switchValue, settings.deviceId)
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] handleGotoEvent: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] handleGotoEvent: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] handleGotoEvent: Exiting.")
}

def handleDeviceStateReport(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int[] messageArgs) {
    logTrace("[${device.deviceNetworkId}] handleDeviceStateReport: Entering with eventSource=%s, eventType=%s, networkId=0x%02X, sourceId=0x%02X, destinationId=0x%02X, args=%s.",
            eventSource, eventType, networkId, sourceId, destinationId, messageArgs)
    try {
        isCorrectParent()
        int channel = settings.channelId.intValue() - 1
        int level = messageArgs.size() > channel ? Math.min(messageArgs[channel], 100) : 0
        def switchValue = (level == 0) ? "off" : "on"
        logInfo("[${device.deviceNetworkId}] handleDeviceStateReport: Updating switch=%s for deviceId=0x%02X.", switchValue, settings.deviceId)
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        logError("[${device.deviceNetworkId}] handleDeviceStateReport: Illegal state: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logError("[${device.deviceNetworkId}] handleDeviceStateReport: Unexpected error: %s.", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
    logTrace("[${device.deviceNetworkId}] handleDeviceStateReport: Exiting.")
}