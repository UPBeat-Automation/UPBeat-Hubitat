/*
* Hubitat Driver: UPB Scene Actuator
* Description: Universal Powerline Bus Scene Actuator Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
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
        input name: "networkId", type: "number", title: "Network ID", required: true, range: "0..255"
        input name: "linkId", type: "number", title: "Link ID", required: true, range: "1..250"
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
        // Initialize the lastTrigger attribute
        sendEvent(name: "lastTrigger", value: "None (not triggered yet)")
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
def activate() {
    logDebug("Sending Activate Link command to scene [${settings.linkId}] on Network ID [${settings.networkId}]")
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before triggering the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        byte[] data = getParent().buildSceneActivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Activate Link [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            // Update lastTrigger with the current timestamp
            String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
            String lastTriggerValue = "Activated at ${timestamp} by user"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
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
        logWarn("Call to scene activate failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene activate failed: ${e.message}", isStateChange: true)
    }
}

def deactivate() {
    logDebug("Sending Deactivate Link command to scene [${settings.linkId}] on Network ID [${settings.networkId}]")
    try {
        isCorrectParent()
        if (settings.networkId == null || settings.linkId == null) {
            logError "Network ID and Link ID must be configured before triggering the scene"
            sendEvent(name: "status", value: "error", descriptionText: "Network ID and Link ID must be configured", isStateChange: true)
            return
        }
        byte[] data = getParent().buildSceneDeactivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Deactivate Link [${data}]")
        if (getParent().sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            // Update lastTrigger with the current timestamp
            String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
            String lastTriggerValue = "Deactivated at ${timestamp} by user"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
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
        logWarn("Call to scene deactivate failed: ${e.message}")
        sendEvent(name: "status", value: "error", descriptionText: "Scene deactivate failed: ${e.message}", isStateChange: true)
    }
}

/***************************************************************************
 * UPB Receive Handlers
 ***************************************************************************/
def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})"
    try {
        isCorrectParent()
        if (settings.networkId != networkId || settings.linkId != linkId) {
            logWarn "Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})"
            return
        }
        boolean success = false
        String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
        switch (eventType) {
            case "activate":
                logDebug "Activating scene [${settings.linkId}] due to Link Event"
                def lastTriggerValue = "Activated at ${timestamp} by ${sourceId}"
                sendEvent(name: "lastTrigger", value: lastTriggerValue)
                logInfo "Scene last trigger updated: ${lastTriggerValue}"
                success = true
                break
            case "deactivate":
                logDebug "Deactivating scene [${settings.linkId}] due to Link Event"
                def lastTriggerValue = "Deactivated at ${timestamp} by ${sourceId}"
                sendEvent(name: "lastTrigger", value: lastTriggerValue)
                logInfo "Scene last trigger updated: ${lastTriggerValue}"
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