/*
 * Hubitat Driver: UPB Single-Speed Fan
 * Description: Universal Powerline Bus UPB Single-Speed Fan Driver (On/Off only)
 * Copyright: 2025 UPBeat Automation
 * Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
 * Author: UPBeat Automation
 */
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

metadata {
    definition(name: "UPB Single-Speed Fan", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "") {
        capability "Switch"
        capability "FanControl"
        capability "Refresh"
        command "setSpeed", [[name:"speed", type: "ENUM", constraints: ["off", "high"], description: "Set the fan speed (off or high)"]]
        attribute "status", "enum", ["ok", "error"]
        attribute "speed", "enum", ["off", "high"]
    }

    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        input name: "networkId", type: "number", title: "Network ID", description: "UPB Network ID (0-255)", required: true, range: "0..255"
        input name: "deviceId", type: "number", title: "Device ID", description: "UPB Device ID (0-255)", required: true, range: "0..255"
        input name: "channelId", type: "number", title: "Channel ID", description: "UPB Channel ID (0-255)", required: true, range: "0..255"
        input name: "receiveComponent1", type: "text", title: "Receive Component 1", description: "Format: linkID:level:rate (e.g., 1:100:0 for on, 1:0:0 for off)"
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
 * Global Static Data
 ***************************************************************************/
@Field static final Map SPEED_TO_LEVEL = [
        "off": 0,
        "high": 100
]

@Field static final List SUPPORTED_SPEEDS = ["off", "high"]

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
        logDebug "Installing UPB Fan Switch"
        device.updateDataValue("receiveComponents", JsonOutput.toJson([:]))
        sendEvent(name: "status", value: "ok", isStateChange: false)
        sendEvent(name: "switch", value: "off", isStateChange: true)
        sendEvent(name: "speed", value: "off", isStateChange: true)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def updated() {
    logTrace "updated()"
    try {
        isCorrectParent()
        def components = [:]
        def errors = []

        (1..16).each { slot ->
            def slotInput = settings."receiveComponent${slot}"?.trim()
            if (slotInput) {
                def parts = slotInput.split(":")
                if (parts.size() < 2 || parts.size() > 3) {
                    errors << "Slot ${slot}: Invalid format: ${slotInput}. Expected linkID:level:rate (rate optional). linkID: 1-250, level: 0 or 100, rate: 0-255."
                    return
                }

                def linkId, level, rate
                try {
                    linkId = parts[0].toInteger()
                    level = parts[1].toInteger()
                    rate = parts.size() == 3 ? parts[2].toInteger() : 0
                } catch (NumberFormatException e) {
                    errors << "Slot ${slot}: Invalid number format in ${slotInput}: ${e.message}."
                    return
                }

                if (linkId < 1 || linkId > 250) {
                    errors << "Slot ${slot}: Invalid linkId: ${linkId}. Must be between 1 and 250."
                    return
                }
                if (level != 0 && level != 100) {
                    errors << "Slot ${slot}: Invalid level: ${level}. Must be 0 or 100 for fan switch."
                    return
                }
                if (rate < 0 || rate > 255) {
                    errors << "Slot ${slot}: Invalid rate: ${rate}. Must be between 0 and 255."
                    return
                }

                def linkIdKey = linkId.toString()
                if (components.containsKey(linkIdKey)) {
                    errors << "Slot ${slot}: Duplicate Link ID ${linkId} already defined in slot ${components[linkIdKey].slot}."
                    return
                }

                components[linkIdKey] = [
                        level: level,
                        rate: rate,
                        slot: slot.toString()
                ]
            }
        }

        if (errors) {
            errors.each { error -> logWarn error }
            throw new Exception("Invalid receive link configuration.")
        }

        device.updateDataValue("receiveComponents", JsonOutput.toJson(components))
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

        def result = getParent().updateDeviceSettings(device, settings)
        if (!result.success) {
            logError "Failed to update device: ${result.error}"
            sendEvent(name: "status", value: "error", descriptionText: result.error, isStateChange: true)
            return
        }

        state.clear()
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
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
        logDebug "Parse called with description: ${description}"
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * Handlers for Driver Data
 ***************************************************************************/
def updateNetworkId(Long networkId) {
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

def updateDeviceId(Long deviceId) {
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

def updateChannelId(Long channelId) {
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
    } catch (Exception e) {
        logWarn "Refresh failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Refresh failed: ${e.message}", isStateChange: true)
    }
}

def on() {
    logDebug "Sending ON to device [${settings.deviceId}]"
    try {
        isCorrectParent()
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 100, 0, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "speed", value: "high", isStateChange: true)
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
        logWarn "On command failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "On command failed: ${e.message}", isStateChange: true)
    }
}

def off() {
    logDebug "Sending OFF to device [${settings.deviceId}]"
    try {
        isCorrectParent()
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), 0, 0, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}]"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            sendEvent(name: "switch", value: "off", isStateChange: true)
            sendEvent(name: "speed", value: "off", isStateChange: true)
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
        logWarn "Off command failed: ${e.message}"
        sendEvent(name: "status", value: "error", descriptionText: "Off command failed: ${e.message}", isStateChange: true)
    }
}

def cycleSpeed() {
    logTrace "cycleSpeed()"
    try {
        isCorrectParent()
        def currentSpeed = device.currentValue("speed") ?: "off"
        logDebug "Current speed: ${currentSpeed}"
        if (currentSpeed == "off") {
            setSpeed("high")
        } else {
            setSpeed("off")
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

def setSpeed(String speed) {
    logDebug "setSpeed(${speed})"
    try {
        isCorrectParent()
        if (!SUPPORTED_SPEEDS.contains(speed)) {
            def error = "Invalid speed: ${speed}. Supported speeds: ${SUPPORTED_SPEEDS.join(', ')}"
            logWarn error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
            return
        }

        def level = SPEED_TO_LEVEL[speed]
        byte[] data = getParent().buildGotoCommand(settings.networkId.intValue(), settings.deviceId.intValue(), level, 0, settings.channelId.intValue())
        logDebug "UPB Command Goto [${data}] for speed ${speed} (level ${level}%)"
        if (getParent().sendPimMessage(data)) {
            logDebug "Command successfully sent [${data}]"
            def switchValue = (speed == "off") ? "off" : "on"
            sendEvent(name: "switch", value: switchValue, isStateChange: true)
            sendEvent(name: "speed", value: speed, isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            def error = "Failed to set speed ${speed} with command [${data}]"
            logDebug error
            sendEvent(name: "status", value: "error", descriptionText: error, isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logDebug "Received UPB scene command for ${device.deviceNetworkId}: Link ID ${linkId}"
    try {
        isCorrectParent()
        def receiveComponents = [:]
        def jsonData = device.getDataValue("receiveComponents")
        if (jsonData) {
            receiveComponents = new JsonSlurper().parseText(jsonData)
        }

        def linkIdKey = linkId.toString()
        def component = receiveComponents?.get(linkIdKey)

        if (component) {
            def level = component.level
            def rate = component.rate
            def slot = component.slot
            logDebug "Executing action for Link ID ${linkId} (Slot ${slot}): Level=${level}, Rate=${rate}"
            def speed = (level == 0) ? "off" : "high"
            setSpeed(speed)
            logDebug "Rate ${rate} not fully implemented; action applied instantly"
        } else {
            logDebug "No action defined for Link ID ${linkId}. Check receive link configuration."
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
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
        def speed = (level == 0) ? "off" : "high"
        logDebug "Updating switch to ${(speed == "off") ? "off" : "on"} and speed to ${speed} for device [${settings.deviceId}]"
        sendEvent(name: "switch", value: (speed == "off") ? "off" : "on", isStateChange: true)
        sendEvent(name: "speed", value: speed, isStateChange: true)
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}