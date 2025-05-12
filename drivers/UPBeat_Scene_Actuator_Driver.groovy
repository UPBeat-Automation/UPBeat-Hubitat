/*
* Hubitat Driver: UPB Scene Actuator
* Description: Univeral Powerline Bus Scene Actuator Driver
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
        // Custom attribute to track the last trigger event and timestamp
        attribute "lastTrigger", "string"
    }
    preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true    
        input name: "networkId", type: "number", title: "Network ID", required: true, range: "0..255"
        input name: "linkId", type: "number", title: "Link ID", required: true, range: "1..250"    
    }
}

void installed() {
    logTrace "installed()"
    // Initialize the lastTrigger attribute
    sendEvent(name: "lastTrigger", value: "None (not triggered yet)")
}

def updated() {
    logTrace "updated()"
    // Validate inputs
    if (settings.networkId == null || settings.networkId < 0 || settings.networkId > 255) {
        logError "Network ID must be between 0 and 255, got: ${settings.networkId}"
        return
    }
    if (settings.linkId == null || settings.linkId < 1 || settings.linkId > 250) {
        logError "Link ID must be between 1 and 250, got: ${settings.linkId}"
        return
    }
    state.clear()
}

def updateNetworkId(Long networkId) {
    logTrace "updateLinkId"
    device.updateSetting("networkId", [type: "number", value: networkId])
}

def updateLinkId(Long deviceId) {
    logTrace "updateLinkId"
    device.updateSetting("linkId", [type: "number", value: deviceId])
}

def parse(String description) {
    logTrace "parse(${description})"
}

def activate() {
    if (settings.networkId == null || settings.linkId == null) {
        logError "Network ID and Link ID must be configured before triggering the scene"
        return
    }

    logDebug("Sending Activate Link command to scene [${settings.linkId}] on Network ID [${settings.networkId}]")
    try {
        byte[] data = parent.buildSceneActivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Activate Link [${data}]")
        if (parent.sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            // Update lastTrigger with the current timestamp
            String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
            String lastTriggerValue = "Activated at ${timestamp} by user"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
        } else {
            logDebug("Failed to issue command [${data}]")
        }
    } catch (Exception e) {
        logWarn("Call to scene activate failed: ${e.message}")
    }
}

def deactivate() {
    if (settings.networkId == null || settings.linkId == null) {
        logError "Network ID and Link ID must be configured before triggering the scene"
        return
    }

    logDebug("Sending Deactivate Link command to scene [${settings.linkId}] on Network ID [${settings.networkId}]")
    try {
        byte[] data = parent.buildSceneDeactivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        logDebug("UPB Command Deactivate Link [${data}]")
        if (parent.sendPimMessage(data)) {
            logDebug("Command successfully sent [${data}]")
            // Update lastTrigger with the current timestamp
            String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
            String lastTriggerValue = "Deactivated at ${timestamp} by user"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
        } else {
            logDebug("Failed to issue command [${data}]")
        }
    } catch (Exception e) {
        logWarn("Call to scene deactivate failed: ${e.message}")
    }
}

def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId}"
    if (settings.networkId != networkId || settings.linkId != linkId) {
        logWarn "Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})"
        return
    }
    String timestamp = new Date().format("yyyy-MM-dd HH:mm:ss")
    switch (eventType) {
        case "activate":
            logDebug "Activating scene [${settings.linkId}] due to Link Event"
        	def lastTriggerValue = "Activated at ${timestamp} by ${sourceId}"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
            break
        case "deactivate":
            logDebug "Deactivating scene [${settings.linkId}] due to Link Event "
            def lastTriggerValue = "Deactivated at ${timestamp} by ${sourceId}"
            sendEvent(name: "lastTrigger", value: lastTriggerValue)
            logInfo "Scene last trigger updated: ${lastTriggerValue}"
            break
        default:
            logWarn "Unknown Link Event type: ${eventType}"
            break
    }
}