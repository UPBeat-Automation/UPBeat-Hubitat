/*
* Hubitat Driver: UPB Scene Switch
* Description: Universal Powerline Bus Scene Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
metadata {
    definition(name: "UPB Scene Switch", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "", canAddDevice: false) {
        capability "Switch"
        attribute "status", "enum", ["ok", "error"]
    }
}

preferences {
    input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
    input name: "networkId", type: "number", title: "Network ID", required: false
    input name: "linkId", type: "number", title: "Link ID", required: false
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
        state.clear()
        sendEvent(name: "status", value: "ok", isStateChange: false)
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

def updateLinkId(Long linkId) {
    logTrace "updateLinkId()"
    try {
        isCorrectParent()
        device.updateSetting("linkId", [type: "number", value: linkId])
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
def on() {
    logDebug("Sending ON to scene [${settings.linkId}]")
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before activating the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        byte[] data = getParent().buildSceneActivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Activate [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            sendEvent(name: "switch", value: "on", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug("Failed to issue command [${data}]")
            sendEvent(name: "status", value: "error", descriptionText: "Failed to send activate command", isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    } catch (Exception e) {
        logWarn("Call to scene on failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene activate failed: ${e.message}", isStateChange: true)
    }
}

def off() {
    logDebug("Sending OFF to scene [${settings.linkId}]")
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before deactivating the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        byte[] data = getParent().buildSceneDeactivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Deactivate [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            sendEvent(name: "switch", value: "off", isStateChange: true)
            sendEvent(name: "status", value: "ok", isStateChange: false)
        } else {
            logDebug("Failed to issue command [${data}]")
            sendEvent(name: "status", value: "error", descriptionText: "Failed to send deactivate command", isStateChange: true)
        }
    } catch (IllegalStateException e) {
        log.error e.message
        sendEvent(name: "status", value: "error", descriptionText: e.message, isStateChange: true)
        return
    } catch (Exception e) {
        logWarn("Call to scene off failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene deactivate failed: ${e.message}", isStateChange: true)
    }
}

def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})"
    try {
        isCorrectParent()
        if (settings.networkId != networkId || settings.linkId != linkId) {
            logWarn "Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})"
            return
        }
        boolean success = false
        switch (eventType) {
            case "activate":
                logDebug "Activating scene [${settings.linkId}] due to Link Event"
                sendEvent(name: "switch", value: "on", isStateChange: true)
                success = true
                break
            case "deactivate":
                logDebug "Deactivating scene [${settings.linkId}] due to Link Event"
                sendEvent(name: "switch", value: "off", isStateChange: true)
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
        return
    }
}