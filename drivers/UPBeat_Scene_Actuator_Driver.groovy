/*
* Hubitat Driver: UPB Scene Actuator
* Description: Universal Powerline Bus Scene Actuator Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatDriverLib

metadata {
    definition(name: "UPB Scene Actuator", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "", canAddDevice: false) {
        capability "Actuator"
        command "activate"
        command "deactivate"
        attribute "lastTrigger", "string"
        attribute "status", "enum", ["ok", "error"]
    }
    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        input name: "networkId", type: "number", title: "Network ID", description: "UPB Network ID (0-255)", required: true, range: "0..255"
        input name: "linkId", type: "number", title: "Link ID", description: "UPB Link ID (1-250)", required: true, range: "1..250"
    }
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed() {
    logTrace "installed()"
    try {
        isCorrectParent()
        // Initialize the lastTrigger attribute
        sendEvent(name: "lastTrigger", value: "None (not triggered yet)")
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
        // Validate inputs
        if (settings.networkId == null || settings.networkId < 0 || settings.networkId > 255) {
            logError "Network ID must be between 0 and 255, got: ${settings.networkId}"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID must be between 0 and 255", isStateChange: true)
            return
        }
        if (settings.linkId == null || settings.linkId < 1 || settings.linkId > 250) {
            logError "Link ID must be between 1 and 250, got: ${settings.linkId}"
            sendEvent(name: "status", value: "error", descriptionText: "Link ID must be between 1 and 250", isStateChange: true)
            return
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
        state.clear()
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
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
    }
}

def updateLinkId(Long linkId) {
    logTrace "updateLinkId()"
    try {
        isCorrectParent()
        device.updateSetting("linkId", [type: "number", value: linkId])
        sendEvent(name: "status", value: "ok", isStateChange: false)
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}

/***************************************************************************
 * Handlers for Driver Capabilities
 ***************************************************************************/
def activate() {
    logTrace("activate()")
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before activating the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        def networkId = settings.networkId.intValue()
        def linkId = settings.linkId.intValue()
        logDebug("Sending activate to scene [${linkId}] on Network ID [${networkId}]")
        byte[] data = getParent().buildSceneActivateCommand(networkId, linkId, 0)
        logDebug("UPB Command Activate [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            getParent().handleLinkEvent("user", "UPB_ACTIVATE_LINK", networkId, 0, linkId)
        } else {
            logDebug("Failed to issue command [${data}]")
            sendEvent(name: "status", value: "error", descriptionText: "Failed to send activate command", isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn("Call to scene activate failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene activate failed: ${e.message}", isStateChange: true)
    }
}

def deactivate() {
    logTrace "deactivate()"
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before deactivating the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        def networkId = settings.networkId.intValue()
        def linkId = settings.linkId.intValue()
        logDebug("Sending deactivate to scene [${linkId}] on Network ID [${networkId}]")
        byte[] data = getParent().buildSceneDeactivateCommand(networkId, linkId, 0)
        logDebug("UPB Command Deactivate [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            getParent().handleLinkEvent("user", "UPB_DEACTIVATE_LINK", networkId, 0, linkId)
        } else {
            logDebug("Failed to issue command [${data}]")
            sendEvent(name: "status", value: "error", descriptionText: "Failed to send deactivate command", isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    } catch (Exception e) {
        logWarn("Call to scene deactivate failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene deactivate failed: ${e.message}", isStateChange: true)
    }
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})"
    try {
        isCorrectParent()
        if (settings.networkId != networkId || settings.linkId != linkId) {
            logWarn "Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})"
            return
        }
        boolean success = false
        String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
        switch (eventType) {
            case "UPB_ACTIVATE_LINK":
                logDebug "Activating scene [${settings.linkId}] due to Link Event"
                def lastTriggerValue = "Activated at ${timestamp} by ${(eventSource == "user") ? eventSource :  sourceId}"
                logDebug "${lastTriggerValue}"
                sendEvent(name: "lastTrigger", value: lastTriggerValue)
                success = true
                break
            case "UPB_DEACTIVATE_LINK":
                logDebug "Deactivating scene [${settings.linkId}] due to Link Event"
                def lastTriggerValue = "Deactivated at ${timestamp} by ${(eventSource == "user") ? eventSource :  sourceId}"
                sendEvent(name: "lastTrigger", value: lastTriggerValue)
                success = true
                break
            default:
                logWarn "Unknown Link Event type: ${eventType}"
                sendEvent(name: "status", value: "error", descriptionText: "Unknown Link Event type: ${eventType}", isStateChange: true)
                return
        }
        if (success) {
            sendEvent(name: "status", value: "ok", isStateChange: false)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
    }
}