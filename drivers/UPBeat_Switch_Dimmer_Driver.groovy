/*
* Hubitat Driver: UPB Dimming Switch
* Description: Universal Powerline Bus Dimming Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib
#include UPBeat.UPBeatDriverLib

import groovy.transform.Field
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

metadata {
    definition(name: "UPB Dimming Switch", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "") {
        capability "Switch"
        capability "SwitchLevel"
        capability "Refresh"
        command "setLevel", [
                [name: "level", type: "NUMBER", description: "Set level (0-100)", constraints: ["MIN": 0, "MAX": 100]],
                [name: "duration", type: "ENUM", description: "Set the fade rate", constraints: FADE_RATE_MAPPING.keySet()]
        ]
        attribute "status", "enum", ["ok", "error"]

    }

    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        input name: "networkId", type: "number", title: "Network ID", description: "UPB Network ID (0-255)", required: true, range: "0..255"
        input name: "deviceId", type: "number", title: "Device ID", description: "UPB Device ID (1-250)", required: true, range: "1..250"
        input name: "channelId", type: "number", title: "Channel ID", description: "UPB Channel ID (1-255)", required: true, range: "1..255"
        input name: "fadeRate", type: "enum", title: "Default Fade Rate", options: FADE_RATE_MAPPING.keySet(), defaultValue: "Default", required: false
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

@Field static Map FADE_RATE_MAPPING = [
        "Snap": 0,  // Instant
        "0.3s": 1,  // 0.3 seconds
        "0.5s": 2,  // 0.5 seconds
        "1s": 3,    // 1 second
        "2s": 4,    // 2 seconds
        "5s": 5,    // 5 seconds
        "10s": 6,   // 10 seconds
        "20s": 7,   // 20 seconds
        "30s": 8,   // 30 seconds
        "40s": 9,   // 40 seconds
        "60s": 10,  // 1 minute
        "90s": 11,  // 1.5 minutes
        "2min": 12, // 2 minutes
        "5min": 13, // 5 minutes
        "30min": 14,// 30 minutes
        "1hr": 15,  // 1 hour
        "Default": 255 // Device programmed default
]

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed() {
    logTrace "installed()"
    try {
        isCorrectParent()
        logDebug "Installing UPB Dimming Switch"
        // Initialize state.receiveComponents to ensure it's not null
        device.updateDataValue("receiveComponents", JsonOutput.toJson([:]))
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updated() {
    logTrace "updated()"
    try {
        isCorrectParent()
        // Validate fadeRate (for dimmable devices)
        if (settings.fadeRate && !(settings.fadeRate in FADE_RATE_MAPPING.keySet())) {
            logWarn "Invalid fade rate: ${settings.fadeRate}. Using device default."
            device.updateSetting("fadeRate", [value: "Default", type: "string"])
        }

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
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn "Failed to update: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * Handlers for Driver Data
 ***************************************************************************/
def updateNetworkId(int networkId) {
    logTrace "updateNetworkId(${networkId})"
    try {
        isCorrectParent()
        device.updateSetting("networkId", [type: "number", value: networkId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updateDeviceId(int deviceId) {
    logTrace "updateDeviceId(${deviceId})"
    try {
        isCorrectParent()
        device.updateSetting("deviceId", [type: "number", value: deviceId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updateChannelId(int channelId) {
    logTrace "updateChannelId(${channelId})"
    try {
        isCorrectParent()
        device.updateSetting("channelId", [type: "number", value: channelId])
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
    logTrace "refresh()"
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
    } catch (Exception e) {
        logWarn "Call to refresh failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Refresh failed: ${e.message}", isStateChange: true)
    }
}

def flash(BigDecimal rateToFlash) {
    logTrace "flash(${rateToFlash})"
    try {
        isCorrectParent()
        logDebug "Flash Rate [${rateToFlash}]"
        byte[] data = getParent().blinkCommand(settings.networkId.intValue(), settings.deviceId.intValue(), rateToFlash, settings.channelId.intValue())
        logDebug "UPB Flash Command[${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "UPB Flash [${data}]"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "level", value: 100, isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue flash command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def on() {
    logTrace "on()"
    try {
        isCorrectParent()
        def rate = settings.fadeRate ? FADE_RATE_MAPPING[settings.fadeRate] : 255
        logDebug "Sending ON to device [${settings.deviceId}] at rate [${rate}]"
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 100, rate, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "level", value: 100, unit: "%", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue on command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn "Call to on failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "On command failed: ${e.message}", isStateChange: true)
    }
}

def off() {
    logTrace "off()"
    try {
        isCorrectParent()
        def rate = settings.fadeRate ? FADE_RATE_MAPPING[settings.fadeRate] : 255
        logDebug "Sending OFF to device [${settings.deviceId}] at rate [${rate}]"
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 0, rate, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "off", isStateChange: true)
            sendEvent(name: "level", value: 0, unit: "%", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue off command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn "Call to off failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Off command failed: ${e.message}", isStateChange: true)
    }
}

def setLevel(value, duration = null) {
    logTrace "setLevel(${value}, ${duration})"
    try {
        isCorrectParent()
        // Validate and clamp level
        def level = value.toInteger()
        level = Math.max(0, Math.min(level, 100))

        // Convert duration to numeric rate
        def rate
        if (duration != null && FADE_RATE_MAPPING.containsKey(duration)) {
            rate = FADE_RATE_MAPPING[duration]
        } else {
            rate = settings.fadeRate && FADE_RATE_MAPPING.containsKey(settings.fadeRate) ? FADE_RATE_MAPPING[settings.fadeRate] : 255
            if (duration != null) {
                logWarn "Invalid duration '${duration}'. Using default fade rate: ${settings.fadeRate ?: 'Default'} (rate=${rate})"
            }
        }

        logDebug "Sending Set-Percent to device [${settings.deviceId}] with level=${level}, rate=${rate} (duration=${duration ?: 'not specified'})"
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), level, rate, settings.channelId.intValue())
        logDebug "UPB Command Goto level [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            // Update switch and level attributes
            sendEvent(name: "switch", value: level > 0 ? "on" : "off", isStateChange: true)
            sendEvent(name: "level", value: level, unit: "%", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to issue setLevel command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn "Call to setLevel failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "SetLevel command failed: ${e.message}", isStateChange: true)
    }
}

/***************************************************************************
 * UPB Receive Component Handlers
 ***************************************************************************/
def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})"
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
                    if (level == 0) {
                        sendEvent(name: "switch", value: "off")
                        sendEvent(name: "level", value: level)
                    } else {
                        sendEvent(name: "switch", value: "on")
                        sendEvent(name: "level", value: level)
                    }
                    break
                case "UPB_DEACTIVATE_LINK":
                    sendEvent(name: "switch", value: "off")
                    sendEvent(name: "level", value: 0)
                    break
                default:
                    sendEvent(name: "status", value: "error", descriptionText: "Unknown event type eventType=${eventType}", isStateChange: true)
                    return
                    break
            }
            sendEvent(name: "lastReceivedLinkId", value: linkId)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug "No action defined for Link ID ${linkId} on ${device.deviceNetworkId}. Check the receive link configuration."
            sendEvent(name: "lastReceivedLinkId", value: linkId)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def handleGotoEvent(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int level, int rate, int channel)
{
    logTrace "handleGotoEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, destinationId=${destinationId}, level=${level}, rate=${rate}, channel=${channel})"
    try {
        isCorrectParent()
        def switchValue = (level == 0) ? "off" : "on"
        logDebug "Updating switch to ${switchValue}, level to ${level} for device [${settings.deviceId}]"
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "level", value: level, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}


def handleDeviceStateReport(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int[] messageArgs) {
    logTrace "handleDeviceEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, destinationId=${destinationId}, args=${messageArgs})"
    try {
        isCorrectParent()
        int channel = settings.channelId.intValue() - 1
        int level = messageArgs.size() > channel ? Math.min(messageArgs[channel], 100) : 0

        def switchValue = (level == 0) ? "off" : "on"
        logDebug "Updating switch to ${switchValue}, level to ${level} for device [${settings.deviceId}]"
        sendEvent(name: "switch", value: switchValue, isStateChange: true)
        sendEvent(name: "level", value: level, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}