/*
* Hubitat Driver: UPB Scene Switch
* Description: Universal Powerline Bus Scene Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatDriverLib

metadata {
    definition(name: "UPB Scene Switch", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "", canAddDevice: false) {
        capability "Switch"
        attribute "status", "enum", ["ok", "error"]
    }
}

preferences {
    input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
    input name: "networkId", type: "number", title: "Network ID", description: "UPB Network ID (0-255)", required: true, range: "0..255"
    input name: "linkId", type: "number", title: "Link ID", description: "UPB Link ID (1-250)", required: true, range: "1..250"
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed() {
    logTrace("installed()")
    try {
        isCorrectParent()
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
        // Validate inputs
        if (settings.networkId == null || settings.networkId < 0 || settings.networkId > 255) {
            logError("Network ID must be between 0 and 255, got: ${settings.networkId}")
            sendEvent(name: "status", value: "error", descriptionText: "Network ID must be between 0 and 255", isStateChange: true)
            return
        }
        if (settings.linkId == null || settings.linkId < 1 || settings.linkId > 250) {
            logError("Link ID must be between 1 and 250, got: ${settings.linkId}")
            sendEvent(name: "status", value: "error", descriptionText: "Link ID must be between 1 and 250", isStateChange: true)
            return
        }
        // Update parent device settings this will updated the device ID
        def result = parent.updateDeviceSettings(device, settings)
        if (result.success) {
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logError("Failed to update device: ${result.error}")
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
    logTrace("updateNetworkId()")
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
    logTrace("updateLinkId()")
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
def on() {
    logTrace("on()")
    try {
        isCorrectParent()
        // Validate inputs
        if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
            logError("Network ID ${settings.networkId} is invalid or out of range (0-255)")
            throw new IllegalArgumentException("Network ID must be 0-255")
        }
        if (!settings.linkId || settings.linkId < 0 || settings.linkId > 255) {
            logError("Link ID ${settings.linkId} is invalid or out of range (0-255)")
            throw new IllegalArgumentException("Link ID must be 0-255")
        }

        def networkId = settings.networkId.intValue()
        def linkId = settings.linkId.intValue()
        logDebug("Sending activate to scene [${linkId}] on Network ID [${networkId}]")
        getParent().activateScene(networkId, linkId, 0)
        logDebug("Scene activation succeeded")
        getParent().handleLinkEvent("user", "UPB_ACTIVATE_LINK", networkId, 0, linkId)
        return true
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        throw e
    } catch (RuntimeException e) {
        logError("Scene activation failed: %s", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        throw e
    } catch (Exception e) {
        logWarn("Scene activation failed: %s", e.message)
        sendEvent(name: "status", value: "error", descriptionText: "Scene activate failed: ${e.message}", isStateChange: true)
        throw e
    }
}

def off() {
    logTrace("off()")
    try {
        isCorrectParent()
        // Validate inputs
        if (!settings.networkId || settings.networkId < 0 || settings.networkId > 255) {
            logError("Network ID ${settings.networkId} is invalid or out of range (0-255)")
            throw new IllegalArgumentException("Network ID must be 0-255")
        }
        if (!settings.linkId || settings.linkId < 0 || settings.linkId > 255) {
            logError("Link ID ${settings.linkId} is invalid or out of range (0-255)")
            throw new IllegalArgumentException("Link ID must be 0-255")
        }

        def networkId = settings.networkId.intValue()
        def linkId = settings.linkId.intValue()
        logDebug("Sending deactivate to scene [${linkId}] on Network ID [${networkId}]")
        getParent().deactivateScene(networkId, linkId, 0)
        logDebug("Scene deactivation succeeded")
        getParent().handleLinkEvent("user", "UPB_DEACTIVATE_LINK", networkId, 0, linkId)
        return true
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        throw e
    } catch (RuntimeException e) {
        logError("Scene deactivation failed: %s", e.message)
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        throw e
    } catch (Exception e) {
        logWarn("Scene deactivation failed: %s", e.message)
        sendEvent(name: "status", value: "error", descriptionText: "Scene deactivate failed: ${e.message}", isStateChange: true)
        throw e
    }
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace("handleLinkEvent(eventSource=${eventSource}, eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})")
    try {
        isCorrectParent()
        if (settings.networkId != networkId || settings.linkId != linkId) {
            logWarn("Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})")
            return
        }
        boolean success = false
        switch (eventType) {
            case "UPB_ACTIVATE_LINK":
                logDebug("Activating scene [${settings.linkId}] due to Link Event")
                sendEvent(name: "switch", value: "on", isStateChange: true)
                sendEvent(name: "status", value: "ok", isStateChange: false)
                success = true
                break
            case "UPB_DEACTIVATE_LINK":
                logDebug("Deactivating scene [${settings.linkId}] due to Link Event")
                sendEvent(name: "switch", value: "off", isStateChange: true)
                sendEvent(name: "status", value: "ok", isStateChange: false)
                success = true
                break
            default:
                logWarn("Unknown Link Event type: ${eventType}")
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