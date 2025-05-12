/*
* Hubitat Driver: UPB Scene Switch
* Description: Univeral Powerline Bus Scene Switch Driver
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
#include UPBeat.UPBeatLogger
metadata {
    definition(name: "UPB Scene Switch", namespace: "UPBeat", author: "UPBeat Automation", importUrl: "", canAddDevice: false) {
        capability "Switch"
    }
}

preferences {
        input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
		input name: "networkId", type: "number", title: "Network ID", required: false
        input name: "linkId", type: "number", title: "Link ID", required: false
}

/***************************************************************************
 * Core Driver Functions
 ***************************************************************************/
void installed()
{
    logTrace "installed()"
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

def parse(String description) {
    logTrace "parse(${description})"
}

def updateNetworkId(Long networkId) {
    logTrace "updateLinkId"
    device.updateSetting("networkId", [type: "number", value: networkId])
}

def updateLinkId(Long deviceId) {
    logTrace "updateLinkId"
    device.updateSetting("linkId", [type: "number", value: deviceId])
}

/***************************************************************************
 * Handlers for Driver Capabilities
 ***************************************************************************/

def on() {
    logDebug("Sending ON to scene [${settings.linkId}]")

    try {
        byte[] data = parent.buildSceneActivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)

        logDebug("UPB Command activate [${data}]");

        if( parent.sendPimMessage(data) ){
            logDebug("Command successfully sent [${data}]");
            sendEvent(name: "switch", value: "on", isStateChange: true);
        }
        else
        {
            logDebug("Failed to issue command [${data}]");
        }
    } catch (Exception e) {
        logWarn("Call to scene on failed: ${e.message}");
    }
}

def off() {
    logDebug("Sending OFF to scene [${settings.linkId}]")

    try {
        byte[] data = parent.buildSceneDeactivateCommand(settings.networkId.intValue(), settings.linkId.intValue(), 0)
        
        logDebug("UPB Command deactivate [${data}]");
        
        if( parent.sendPimMessage(data) ){
            logDebug("Command successfully sent [${data}]");
            sendEvent(name: "switch", value: "off", isStateChange: true);
        }
        else
        {
            logDebug("Failed to issue command [${data}]");
        }
    } catch (Exception e) {
        logWarn("Call to scene off failed: ${e.message}");
    }
}

def handleLinkEvent(String eventType, int networkId, int sourceId, int linkId) {
    logTrace "handleLinkEvent(eventType=${eventType}, networkId=${networkId}, sourceId=${sourceId}, linkId=${linkId})"
    if (settings.networkId != networkId || settings.linkId != linkId) {
        logWarn "Received Link Event for incorrect Network ID (${networkId} vs ${settings.networkId}) or Link ID (${linkId} vs ${settings.linkId})"
        return
    }
    switch (eventType) {
        case "activate":
            logDebug "Activating scene [${settings.linkId}] due to Link Event"
            sendEvent(name: "switch", value: "on", isStateChange: true)
            break
        case "deactivate":
            logDebug "Deactivating scene [${settings.linkId}] due to Link Event"
            sendEvent(name: "switch", value: "off", isStateChange: true)
            break
        default:
            logWarn "Unknown Link Event type: ${eventType}"
            break
    }
}