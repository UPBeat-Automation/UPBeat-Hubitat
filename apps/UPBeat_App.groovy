/*
* Hubitat App: UPBeat App
* Description: Hubitat App for Univeral Powerline Bus Support
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import hubitat.helper.HexUtils
import groovy.transform.Field
import java.util.concurrent.ConcurrentHashMap
import java.security.MessageDigest
import java.util.UUID

#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib
#include UPBeat.UPBProtocolLib

@Field static Map DEVICE_TYPES = [
        "non_dimming_switch": [
                displayName: "UPB Non-Dimming Switch",
                driverName: "UPB Non-Dimming Switch",
                category: "device",
                requiredInputs: [
                        [name: "deviceId", type: "number", title: "Device ID", range: "1..250", required: true],
                        [name: "channelId", type: "number", title: "Channel ID", range: "0..255", defaultValue: 1, required: true]
                ]
        ],
        "single_speed_fan": [
                displayName: "UPB Single-Speed Fan",
                driverName: "UPB Single-Speed Fan",
                category: "device",
                requiredInputs: [
                        [name: "deviceId", type: "number", title: "Device ID", range: "1..250", required: true],
                        [name: "channelId", type: "number", title: "Channel ID", range: "0..255", defaultValue: 1, required: true]
                ]
        ],
        "dimming_switch": [
                displayName: "UPB Dimming Switch",
                driverName: "UPB Dimming Switch",
                category: "device",
                requiredInputs: [
                        [name: "deviceId", type: "number", title: "Device ID", range: "1..250", required: true],
                        [name: "channelId", type: "number", title: "Channel ID", range: "0..255", defaultValue: 1, required: true]
                ]
        ],
        "multi_speed_fan": [
                displayName: "UPB Multi-Speed Fan",
                driverName: "UPB Multi-Speed Fan",
                category: "device",
                requiredInputs: [
                        [name: "deviceId", type: "number", title: "Device ID", range: "1..250", required: true],
                        [name: "channelId", type: "number", title: "Channel ID", range: "0..255", defaultValue: 1, required: true]
                ]
        ],
        "scene_switch": [
                displayName: "UPB Scene Switch",
                driverName: "UPB Scene Switch",
                category: "scene",
                requiredInputs: [
                        [name: "linkId", type: "number", title: "Link ID", range: "1..250", required: true]
                ]
        ],
        "scene_actuator": [
                displayName: "UPB Scene Actuator",
                driverName: "UPB Scene Actuator",
                category: "scene",
                requiredInputs: [
                        [name: "linkId", type: "number", title: "Link ID", range: "1..250", required: true]
                ]
        ]
]

definition(
        name: "UPBeat App",
        namespace: "UPBeat",
        author: "UPBeat Automation",
        description: "Configure Hubitat for UPB Support",
        category: "Convenience",
        iconUrl: "",
        iconX2Url: "",
        iconX3Url: "",
        singleInstance: true
)

preferences {
    page(name: "mainPage")
    page(name: "addDevicePage")
    page(name: "createDevice")
    page(name: "bulkImportPage")
    page(name: "bulkImport")
}

mappings {
    path("/status") {
        action: [
                GET: "handleStatus"
        ]
    }
    path("/device") {
        action: [
                POST: "handleAddDevice"
        ]
    }
    path("/scene") {
        action: [
                POST: "handleAddScene"
        ]
    }
    path("/pim") {
        action: [
                POST: "handleUpdatePowerlineInterface"
        ]
    }
}

/***************************************************************************
 * Custom Application Configuration Pages
 ***************************************************************************/
def addDevicePage() {
    dynamicPage(name: "addDevicePage", title: "Manually Add Device", install: false, uninstall: false, nextPage: "createDevice") {
        section("Create a New Device") {
            // Generate enum options for deviceType
            def deviceTypeOptions = DEVICE_TYPES.collectEntries { key, config -> [(key): config.displayName] }
            input name: "deviceType", type: "enum", title: "Device Type", options: deviceTypeOptions, required: true, submitOnChange: true

            if (settings.deviceType && DEVICE_TYPES[settings.deviceType]) {
                // Common inputs for all device types
                input name: "deviceName", type: "text", title: "Device Name", required: true, submitOnChange: false
                input name: "voiceName", type: "text", title: "Voice Name", required: false, submitOnChange: false
                input name: "networkId", type: "number", title: "Network ID", required: true, range: "0..255", submitOnChange: false

                // Dynamically render inputs based on device type
                DEVICE_TYPES[settings.deviceType].requiredInputs.each { inputConfig ->
                    input(inputConfig + [submitOnChange: false])
                }
            }
        }
    }
}

def bulkImportPage() {
    dynamicPage(name: "bulkImportPage", title: "Bulk Import", install: false, uninstall: false, nextPage: "bulkImport") {
        section() {
            input name: "upeFileData",
                    type: "textarea",
                    title: "UPE File Data",
                    description: "Paste UPStart UPE file here",
                    defaultValue: "",
                    required: true,
                    rows: 20,
                    cols: 80,
                    submitOnChange: true
        }
    }
}

def bulkImport() {
    return dynamicPage(name: "bulkImport", title: "Device Import Completed", nextPage: "mainPage") {
        section("Import Result") {
            //paragraph "${settings.upeFileData}"
            def data = processUpeFile(settings.upeFileData)
            deleteAllDevices()

            // Create Link Devices
            data.links.each { link ->
                def deviceNetworkId = buildSceneNetworkId( data.systemInfo.networkId, link.linkId)
                def sceneName = link.name.trim().tokenize().collect { it.capitalize() }.join(' ')
                paragraph "Adding link device [${deviceNetworkId}] with scene name [${sceneName}]"
                childDevice = addChildDevice("UPBeat", "UPB Scene Actuator" , deviceNetworkId, [name: sceneName, label: sceneName])
                childDevice.updateNetworkId(data.systemInfo.networkId)
                childDevice.updateLinkId(link.linkId)
            }

            // Create Switch Devices
            data.modules.each { module ->
                module.channelInfo.each { channel ->
                    def channelId = channel.channelId + 1
                    def deviceNetworkId = buildDeviceNetworkId(module.networkId, module.moduleId, channelId)
                    def deviceName = "${module.roomName} ${module.deviceName}".trim().tokenize().collect { it.capitalize() }.join(' ')
                    if( channel.dimEnabled ){
                        paragraph "Adding dimming switch [${deviceNetworkId}] with device name [${deviceName}]"
                        childDevice = addChildDevice("UPBeat", "UPB Dimming Switch" , deviceNetworkId, [name: deviceName, label: deviceName])
                        childDevice.updateNetworkId(module.networkId)
                        childDevice.updateDeviceId(module.moduleId)
                        childDevice.updateChannelId(channelId)
                    } else {
                        paragraph "Adding non-dimming switch [${deviceNetworkId}] with device name [${deviceName}]"
                        childDevice = addChildDevice("UPBeat", "UPB Non-Dimming Switch" , deviceNetworkId, [name: deviceName, label: deviceName])
                        childDevice.updateNetworkId(module.networkId)
                        childDevice.updateDeviceId(module.moduleId)
                        childDevice.updateChannelId(channelId)
                    }

                    def device = getChildDevice(deviceNetworkId)
                    if (device){
                        // Populate receive components
                        module.presetInfo.each { preset ->
                            if ( preset.channelId == channel.channelId && preset.linkId != 255 && preset.presetDimLevel != 255)
                            {
                                paragraph "Adding preset to [${deviceNetworkId}] with device name [${deviceName}] at slot ${preset.componentId}  ${preset.linkId}:${preset.presetDimLevel}"
                                device.updateReceiveComponentSlot(preset.componentId + 1, preset.linkId, preset.presetDimLevel)
                            }
                        }
                        def components = device.getReceiveComponents()
                        device.updateDataValue("receiveComponents", JsonOutput.toJson(components))
                    }
                }
            }
        }
    }
}

def createDevice() {
    logTrace("createDevice")

    // Validate common inputs
    if (!settings.deviceType || !settings.deviceName || !settings.networkId) {
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "Device Type, Device Name, and Network ID are required."
            }
        }
    }

    // Validate device-type-specific inputs
    def deviceConfig = DEVICE_TYPES[settings.deviceType]
    if (!deviceConfig) {
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "Invalid Device Type selected."
            }
        }
    }

    def missingInputs = deviceConfig.requiredInputs.findAll { inputConfig -> !settings[inputConfig.name] }
    if (missingInputs) {
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "Missing required inputs: ${missingInputs.collect { it.title }.join(', ')}."
            }
        }
    }

    // Generate deviceNetworkId based on device category
    def deviceNetworkId
    if (deviceConfig.category == "scene") {
        deviceNetworkId = buildSceneNetworkId(settings.networkId.intValue(), settings.linkId.intValue())
    } else {
        deviceNetworkId = buildDeviceNetworkId(settings.networkId.intValue(), settings.deviceId.intValue(), settings.channelId.intValue())
    }

    // Check for duplicate device
    def existingDevice = getChildDevice(deviceNetworkId)
    if (existingDevice) {
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "A device with Network ID ${settings.networkId}, ${deviceConfig.category == 'scene' ? 'Link ID' : 'Device ID'} ${settings[deviceConfig.category == 'scene' ? 'linkId' : 'deviceId']}, and Channel ID ${settings.channelId ?: 'N/A'} already exists."
            }
        }
    }

    // Create the device
    def childDevice
    try {
        childDevice = addChildDevice("UPBeat", deviceConfig.driverName, deviceNetworkId, [name: settings.deviceName, label: settings.voiceName ?: settings.deviceName])
    } catch (Exception e) {
        logError("Failed to create device: ${e.message}")
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "Failed to create the device: ${e.message}"
            }
        }
    }

    // Configure the device based on category
    childDevice.updateNetworkId(settings.networkId.intValue())
    if (deviceConfig.category == "scene") {
        childDevice.updateLinkId(settings.linkId.intValue())
    } else {
        childDevice.updateDeviceId(settings.deviceId.intValue())
        childDevice.updateChannelId(settings.channelId.intValue())
    }

    // Retrieve the device to get its numerical ID
    def createdDevice = getChildDevice(deviceNetworkId)
    if (!createdDevice) {
        logError("Failed to retrieve newly created device with network ID ${deviceNetworkId}")
        return dynamicPage(name: "createDevice", title: "Device Creation Failed", nextPage: "mainPage") {
            section("Error") {
                paragraph "Failed to retrieve the newly created device."
            }
        }
    }

    // Construct the device page URL using the device's numerical ID
    def deviceId = createdDevice.id
    def devicePageUrl = "/device/edit/${deviceId}"

    // Clear all settings
    app.removeSetting("deviceName")
    app.removeSetting("voiceName")
    app.removeSetting("networkId")
    app.removeSetting("deviceType")
    deviceConfig.requiredInputs.each { app.removeSetting(it.name) }

    // Display a confirmation page with a link to the device page
    return dynamicPage(name: "createDevice", title: "Device Created Successfully", nextPage: "mainPage") {
        section("Device Created") {
            paragraph "The device '${settings.deviceName}' has been created successfully."
            href(name: "devicePageLink", title: "Go to Device Page", url: devicePageUrl, description: "Click here to view and configure the newly created device.")
        }
    }
}

def mainPage() {
    getHubUrl()
    dynamicPage(install: true, uninstall: true) {
        /*
		// Section removed until the configuration app is ready.
        section("UPBeat Configuration") {
            if (enableConfig) {
                if (!state.accessToken) {
                    try {
                        createAccessToken()
                    }
                    catch (Exception e) {
                        paragraph("Opps. ${e.message}")
                    }
                }
                if (state.accessToken) {
                    paragraph("""<table style="padding:0px; white-space: nowrap">
                                    <tr>
                                        <td style="text-align: right; padding-right: 10px;">
                                            <strong>API Url:</strong>
                                        </td>
                                        <td style="text-align: left; padding-left: 10px;">
                                            ${getFullLocalApiServerUrl()}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td style="text-align: right; padding-right: 10px;">
                                            <strong>API Token:</strong>
                                        </td>
                                        <td style="text-align: left; padding-left: 10px;">
                                            ${state.accessToken}
                                        </td>
                                    </tr>
                                </table>""")
                }
            } else {
                state.remove("accessToken")
                paragraph("Before enabling Remote Configuration, please be sure you have enabled OAuth.")
                paragraph("The setting is located under \"Apps Code\" > \"UPBeat App\" in the code view.")
            }
            input name: "enableConfig", type: "bool", title: "Enable Remote Configuration", defaultValue: false, submitOnChange: true
        }
        */
        section("Bulk Device Actions"){
            input "refreshAllDeviceStates", "button", title: "Refresh All Device States"

            def logLevels = [:]

            LOG_LEVELS.values().each { level ->
                logLevels.putIfAbsent(level, 0)
            }

            def devices = app.getChildDevices()
            devices.each { device ->
                logLevels[LOG_LEVELS[device.getSetting("logLevel").toInteger()]] += 1
            }

            def formattedLogLevels = logLevels.collect { level, count -> "- ${level}: ${count}"}.join("\n")
            paragraph "Device count by log level:\n${formattedLogLevels}"

            input name: "logLevelGlobal", type: "enum", options: LOG_LEVELS, title: "Global Log Level", description: "Select a log level for all devices", required: false, submitOnChange: true
            if(logLevelGlobal){
                input "setLogLevelGlobal", "button", title: "Apply Log Level Globally"
            }
        }
        section("Device Management") {
            if (app.getInstallationState() == "COMPLETE") {
                href(name: "manualAddHref", title: "Manually Add Device", page: "addDevicePage", description: "Add a device manually")
                href(name: "manualAddHref", title: "Bulk Import", page: "bulkImportPage", description: "Import UPStart export file")
            } else {
                paragraph "Please save the app by clicking 'Done' before adding devices."
            }
        }
        section("Troubleshooting") {
            input name: "logLevel", type: "enum", options: LOG_LEVELS, title: "Log Level", defaultValue: LOG_DEFAULT_LEVEL, required: true
        }
    }
}
/***************************************************************************
 * Global Static Data
 ***************************************************************************/
@Field static String pimDeviceId = "UPBeat_PIM"

/***************************************************************************
 * Core App Functions
 ***************************************************************************/
void installed() {
    logTrace("installed()")
    initialize()
}

void uninstalled() {
    logTrace("uninstalled()")
    unsubscribe()
}

void updated() {
    logTrace("updated()")
    initialize()
}

def initialize() {
    logTrace("initialize()")
    getPimDevice()
    // Clear existing subscriptions to prevent duplicates
    unsubscribe()
}

/***************************************************************************
 * App Helper Functions
 ***************************************************************************/
void appButtonHandler(button) {
    logTrace("appButtonHandler(%s)", button)
    switch(button) {
        case "addDeviceBtn":
            logTrace("createDevice")
            // Validate inputs based on device type
            if (!settings.deviceType || !settings.deviceName || !settings.networkId) {
                logError("Device Type, Device Name, and Network ID are required.")
                return
            }
            if (settings.deviceType != "UPB Scene") {
                if (!settings.deviceId || !settings.channelId) {
                    logError("Device ID and Channel ID are required for ${settings.deviceType} devices.")
                    return
                }
            } else {
                if (!settings.linkId) {
                    logError("Link ID is required for UPB Scene devices.")
                    return
                }
            }

            // Generate deviceNetworkId based on device type
            def deviceNetworkId
            if (settings.deviceType == "UPB Scene") {
                deviceNetworkId = buildSceneNetworkId(settings.networkId, settings.linkId)
            } else {
                deviceNetworkId = buildDeviceNetworkId(settings.networkId, settings.deviceId, settings.channelId)
            }

            // Check for duplicate device
            def existingDevice = getChildDevice(deviceNetworkId)
            if (existingDevice) {
                logError("A device with Network ID ${settings.networkId}, Device/Link ID ${settings.deviceType == 'UPB Scene' ? settings.linkId : settings.deviceId}, and Channel ID ${settings.channelId ?: 'N/A'} already exists.")
                return
            }

            // Create the device
            def childDevice
            try {
                childDevice = addChildDevice("UPBeat", settings.deviceType, deviceNetworkId, [name: settings.deviceName, label: settings.voiceName ?: settings.deviceName])
            } catch (Exception e) {
                logError("Failed to create device: ${e.message}")
                return
            }

            // Configure the device based on type
            if (settings.deviceType == "UPB Scene") {
                childDevice.updateNetworkId(settings.networkId)
                childDevice.updateLinkId(settings.linkId)
            } else {
                childDevice.updateNetworkId(settings.networkId)
                childDevice.updateDeviceId(settings.deviceId)
                childDevice.updateChannelId(settings.channelId)
            }

            // Clear the form settings
            app.removeSetting("deviceName")
            app.removeSetting("voiceName")
            app.removeSetting("networkId")
            app.removeSetting("deviceId")
            app.removeSetting("channelId")
            app.removeSetting("deviceType")
            app.removeSetting("linkId")
            break
        case "refreshAllDeviceStates":
            refreshAllDeviceStates()
            break
        case "setLogLevelGlobal":
            setLogLevelGlobal()
            break
    }
}

def getPimDevice()
{
    logTrace("getPimDevice()")

    def pim = getChildDevice(pimDeviceId)

    if (pim == null) {
        logDebug("Creating PIM device")
        pim = addChildDevice("UPBeat", "UPB Powerline Interface Module", pimDeviceId, [name: "UPB Powerline Interface Module"])
    }

    return pim
}

private String makeUri(String extraPath) {
    logTrace("makeUri()")
    return getFullLocalApiServerUrl() + extraPath + "?access_token=${state.accessToken}"
}

String getHubUrl() {
    def localIP = location.hub.localIP
    def hubUrl = "https://${localIP}"
    return hubUrl
}

void updatePIMDevice(String ipAddress, int portNumber) {
    logTrace("updatePIMDevice()")
    def pim = getPimDevice()
    // Set the device IP
    device.updateSetting("ipAddress", [value: ipAddress, type: "text"])
    device.updateSetting("portNumber", [value: portNumber, type: "number"])
    device.updated()
}

void deleteAllDevices() {
    logTrace("deleteAllDevices()")
    def devices = app.getChildDevices()
    // Delete all child devices except PIM
    devices.each { device ->
        if (device.typeName != "UPB Powerline Interface Module") {
            logDebug("Deleting ${device.deviceNetworkId}")
            deleteChildDevice(device.deviceNetworkId)
        }
    }
}

def refreshAllDeviceStates() {
    logTrace("refreshAllDeviceStates()")
    def devices = app.getChildDevices()
    devices.each { device ->
        if (device.typeName != "UPB Powerline Interface Module" && !device.typeName.contains("Scene")) {
            logInfo("Refreshing ${device.deviceNetworkId} [${device.name}]")
            device.refresh()
            pauseExecution(1000)
        }
    }
}

def setLogLevelGlobal() {
    logTrace("setLogLevelGlobal(${logLevelGlobal})")
    if (!LOG_LEVELS.containsKey(logLevelGlobal.toInteger())) {
        logError("Invalid global log level: ${logLevelGlobal}")
        return [result: false, reason: "Invalid global log level"]
    }
    def devices = app.getChildDevices()
    devices.each { device ->
        logInfo("Setting log level [${device.name}] to ${LOG_LEVELS[logLevelGlobal.toInteger()]}")
        device.updateSetting("logLevel", [value: logLevelGlobal, type: "enum"])
    }
    app.clearSetting("logLevelGlobal")
}

/***************************************************************************
 * Web Service Handlers for Configuration Application
 ***************************************************************************/
void handleStatus() {
    logTrace("handleStatus()")

    def data = [
            message: "UPBeat is alive an well."
    ]

    // Using JsonBuilder to convert the data map to a JSON string
    def json = new JsonBuilder(data).toPrettyString()

    render contentType: "application/json", data: json, status: 200
}

void handleAddDevice() {
    logTrace("handleAddDevice()")

    def postData = request.JSON

    logDebug("Received POST data: ${postData}")

    if ('DeviceInfo' in postData) {
        def result = addDevice(postData['DeviceInfo'])
        def data = [
                message: result
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 200
    } else {
        def data = [
                error: "Invalid data received."
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 400
    }
}

void handleAddScene() {
    logTrace("handleAddScene()")

    def postData = request.JSON

    logDebug("Received POST data: ${postData}")

    if ('LinkInfo' in postData) {
        def data = [
                message: "Data received successfully."
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 200
    } else {
        def data = [
                error: "Invalid data received."
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 400
    }
}

void handleUpdatePowerlineInterface() {
    logTrace("handleUpdatePowerlineInterface()")

    def postData = request.JSON

    logDebug("Received POST data: ${postData}")

    if ('PowerlineInterfaceInfo' in postData) {
        updatePIMDevice(postData['PowerlineInterfaceInfo']['IpAddress'], postData['PowerlineInterfaceInfo']['PortNumber'])

        def data = [
                message: "PIM Updated to ${postData['PowerlineInterfaceInfo']['IpAddress']}:${postData['PowerlineInterfaceInfo']['PortNumber']}"
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 200
    } else {
        def data = [
                error: "Invalid data received."
        ]
        def json = new JsonBuilder(data).toPrettyString()
        render contentType: "application/json", data: json, status: 400
    }
}

/***************************************************************************
 * Custom App Functions
 ***************************************************************************/
def activateScene(Integer networkId, Integer linkId, Integer sourceId) {
    logTrace("activateScene(networkId=0x%02X, linkId=0x%02X, sourceId=0x%02X)", networkId, linkId, sourceId)

    // Validate inputs
    if (networkId < 0 || networkId > 255) {
        logError("Network ID ${networkId} is out of range (0-255)")
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (linkId < 0 || linkId > 255) {
        logError("Link ID ${linkId} is out of range (0-255)")
        return [result: false, reason: "Link ID must be 0-255"]
    }
    if (sourceId < 0 || sourceId > 255) {
        logError("Source ID ${sourceId} is out of range (0-255)")
        return [result: false, reason: "Source ID must be 0-255"]
    }

    def controlWord = encodeControlWord(LNK_LINK, REPRQ_NONE, ACKRQ_NONE, CNT_ONE, SEQ_ZERO)
    logDebug("Activating scene with controlWord=0x%04X", controlWord)
    def result = pimDevice.transmitMessage(controlWord, (byte) networkId, (byte) linkId, (byte) sourceId, UPB_ACTIVATE_LINK, null)

    if (result.result) {
        logDebug("Scene activation succeeded")
    } else {
        logError("Scene activation failed: %s", result.reason)
    }
    return result
}

def deactivateScene(Integer networkId, Integer linkId, Integer sourceId) {
    logTrace("deactivateScene(networkId=0x%02X, linkId=0x%02X, sourceId=0x%02X)", networkId, linkId, sourceId)

    // Validate inputs
    if (networkId < 0 || networkId > 255) {
        logError("Network ID ${networkId} is out of range (0-255)")
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (linkId < 0 || linkId > 255) {
        logError("Link ID ${linkId} is out of range (0-255)")
        return [result: false, reason: "Link ID must be 0-255"]
    }
    if (sourceId < 0 || sourceId > 255) {
        logError("Source ID ${sourceId} is out of range (0-255)")
        return [result: false, reason: "Source ID must be 0-255"]
    }

    def controlWord = encodeControlWord(LNK_LINK, REPRQ_NONE, ACKRQ_NONE, CNT_ONE, SEQ_ZERO)
    logDebug("Deactivating scene with controlWord=0x%04X", controlWord)
    def result = pimDevice.transmitMessage(controlWord, (byte) networkId, (byte) linkId, (byte) sourceId, UPB_DEACTIVATE_LINK, null)

    if (result.result) {
        logDebug("Scene deactivation succeeded")
    } else {
        logError("Scene deactivation failed: %s", result.reason)
    }
    return result
}

def gotoLevel(Integer networkId, Integer deviceId, Integer sourceId, Integer level, Integer duration, Integer channel) {
    logTrace("gotoLevel(networkId=0x%02X, deviceId=0x%02X, sourceId=0x%02X, level=%d, duration=%d, channel=%d)",
            networkId, deviceId, sourceId, level, duration, channel)

    // Validate inputs
    if (networkId < 0 || networkId > 255) {
        logError("Network ID ${networkId} is out of range (0-255)")
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (deviceId < 0 || deviceId > 255) {
        logError("Device ID ${deviceId} is out of range (0-255)")
        return [result: false, reason: "Device ID must be 0-255"]
    }
    if (sourceId < 0 || sourceId > 255) {
        logError("Source ID ${sourceId} is out of range (0-255)")
        return [result: false, reason: "Source ID must be 0-255"]
    }
    if (level < 0 || level > 100) {
        logError("Level ${level} is out of range (0-100)")
        return [result: false, reason: "Level must be 0-100"]
    }
    if (duration < 0 || duration > 255) {
        logError("Duration ${duration} is out of range (0-255)")
        return [result: false, reason: "Duration must be 0-255"]
    }
    if (channel < 0 || channel > 255) {
        logError("Channel ${channel} is out of range (0-255)")
        return [result: false, reason: "Channel must be 0-255"]
    }

    def controlWord = encodeControlWord(LNK_DIRECT, REPRQ_NONE, ACKRQ_PULSE, CNT_ONE, SEQ_ZERO)
    logDebug("Setting level with controlWord=0x%04X", controlWord)
    def result = pimDevice.transmitMessage(controlWord, (byte) networkId, (byte) deviceId, (byte) sourceId, UPB_GOTO, [(byte) level, (byte) duration, (byte) channel] as byte[])

    if (result.result) {
        logDebug("Goto level succeeded")
    } else {
        logError("Goto level failed: %s", result.reason)
    }
    return result
}

def requestDeviceState(Integer networkId, Integer deviceId, Integer sourceId) {
    logTrace("requestDeviceState(networkId=0x%02X, deviceId=0x%02X, sourceId=0x%02X)", networkId, deviceId, sourceId)

    // Validate inputs
    if (networkId < 0 || networkId > 255) {
        logError("Network ID ${networkId} is out of range (0-255)")
        return [result: false, reason: "Network ID must be 0-255"]
    }
    if (deviceId < 0 || deviceId > 255) {
        logError("Device ID ${deviceId} is out of range (0-255)")
        return [result: false, reason: "Device ID must be 0-255"]
    }
    if (sourceId < 0 || sourceId > 255) {
        logError("Source ID ${sourceId} is out of range (0-255)")
        return [result: false, reason: "Source ID must be 0-255"]
    }

    def controlWord = encodeControlWord(LNK_DIRECT, REPRQ_NONE, ACKRQ_PULSE, CNT_ONE, SEQ_ZERO)
    logDebug("Requesting device state with controlWord=0x%04X", controlWord)
    def result = pimDevice.transmitMessage(controlWord, (byte) networkId, (byte) deviceId, (byte) sourceId, UPB_REPORT_STATE, null)

    if (result.result) {
        logDebug("Device state request succeeded")
    } else {
        logError("Device state request failed: %s", result.reason)
    }
    return result
}

/***************************************************************************
 * Custom App Functions
 ***************************************************************************/
def updateDeviceSettings(device, settings) {
    logTrace("updateDeviceSettings(${device.deviceNetworkId})")
    if (!settings) {
        logError("Cannot update device ${device.deviceNetworkId}: Settings are null.")
        return [success: false, error: "Settings are null"]
    }
    try {
        // Update deviceNetworkId
        def deviceConfig = DEVICE_TYPES.find { it.value.driverName == device.typeName }?.value
        if (!deviceConfig) {
            logError("Cannot update device ${device.deviceNetworkId}: Unknown device type.")
            return [success: false, error: "Unknown device type"]
        }
        def newDeviceNetworkId
        if (deviceConfig.category == "scene") {
            if (!settings.networkId || !settings.linkId) {

                logError("Cannot update deviceNetworkId for ${device.deviceNetworkId}: Missing networkId or linkId.")
                return [success: false, error: "Missing networkId or linkId"]
            }
            newDeviceNetworkId = buildSceneNetworkId(settings.networkId.intValue(), settings.linkId.intValue())
        } else {
            if (!settings.networkId || !settings.deviceId || !settings.channelId) {

                logError("Cannot update deviceNetworkId for ${device.deviceNetworkId}: Missing networkId, deviceId, or channelId.")
                return [success: false, error: "Missing networkId, deviceId, or channelId"]
            }
            newDeviceNetworkId = buildDeviceNetworkId(settings.networkId.intValue(), settings.deviceId.intValue(), settings.channelId.intValue())
        }
        if (newDeviceNetworkId != device.deviceNetworkId) {
            def existingDevice = getChildDevice(newDeviceNetworkId)
            if (existingDevice && existingDevice.id != device.id) {
                logError("Cannot update deviceNetworkId for ${device.deviceNetworkId}: ${newDeviceNetworkId} conflicts with existing device.")
                return [success: false, error: "Device ID conflict: ${newDeviceNetworkId} is already in use"]
            }
            device.deviceNetworkId = newDeviceNetworkId
            logDebug("Updated deviceNetworkId to ${newDeviceNetworkId} for ${device.deviceNetworkId}")
        }
        logDebug("Updated device ${device.deviceNetworkId} settings")
        return [success: true, error: null]
    } catch (Exception e) {
        logError("Failed to update device ${device.deviceNetworkId}: ${e.message}")
        return [success: false, error: "Failed to update device: ${e.message}"]
    }
}

void addDevice(deviceInfo) {
    deviceInfo['ChannelInfo'].each { channelInfo ->
        // Generate a unique device id based on UPBeat / UPStart Data
        deviceNetworkId = buildDeviceNetworkId(deviceInfo.NetworkId, deviceInfo.ModuleId, channelInfo.ChannelId)

        if (channelInfo.Enabled) {
            deviceFullName = "${deviceInfo.RoomName} ${deviceInfo.DeviceName}${(channelInfo.ChannelId == 0) ? '' : channelInfo.ChannelId}"
            if (channelInfo.VoiceName.isEmpty())
                channelInfo.VoiceName = deviceFullName

            device = getChildDevice(deviceNetworkId)

            if (device == null) {
                if (channelInfo.DimEnabled == 1)
                    addChildDevice("UPBeat", "UPB Dimming Switch", deviceNetworkId, [label: channelInfo.VoiceName, name: deviceFullName, moduleInfo: deviceInfo])
                else
                    addChildDevice("UPBeat", "UPB Non-Dimming Switch", deviceNetworkId, [label: channelInfo.VoiceName, name: deviceFullName, moduleInfo: deviceInfo])

                device = getChildDevice(deviceNetworkId)

                // Let's request the device state in the future
                device.sendEvent(name: "switch", value: "off", isStateChange: false)
                skipEvent = true

            } else {
                logInfo("Device ${deviceNetworkId} already exists")
            }
        } else {
            logInfo("Skipping ${deviceNetworkId} not enabled")
        }
    }
}

def handleLinkEvent(String eventSource, String eventType, int networkId, int sourceId, int linkId) {
    logTrace("handleLinkEvent(eventSource: ${eventSource}, eventType: ${eventType}, networkId: ${networkId}, sourceId: ${sourceId}, linkId: ${linkId})")
    def startTime = now()
    try {
        // Enumerate all child devices, call handleLinkEvent if supported
        def deviceCount = 0
        def processedCount = 0
        getChildDevices().each { device ->
            deviceCount++
            if (device.name != "UPB Powerline Interface Module") {
                try {
                    device.handleLinkEvent(eventSource, eventType, networkId, sourceId, linkId)
                    processedCount++
                    logDebug("Dispatched handleLinkEvent(eventSource: ${eventSource}, eventType: ${eventType}, networkId: ${networkId}, sourceId: ${sourceId}, linkId: ${linkId}) on device ${device.label ?: device.name} (deviceId: ${device.getSetting('deviceId')})")
                } catch (Exception e) {
                    logWarn("Error calling handleLinkEvent on device ${device.label ?: device.name}: ${e.message}")
                }
            } else {
                logDebug("Skipped device ${device.label ?: device.name}: ${device.name == 'UPB Powerline Interface Module' ? 'PIM device' : 'lacks handleLinkEvent method'}")
            }
        }
        def elapsedTime = now() - startTime
        logDebug("Processed link event for linkId ${linkId}: ${processedCount} of ${deviceCount} devices in ${elapsedTime}ms")
    } catch (Exception e) {
        logWarn("Failed to process PIM link event: ${e.message}")
    }
}

def handleDeviceEvent(String eventSource, String eventType, int networkId, int sourceId, int destinationId, int[] messageArgs) {
    logTrace("handleDeviceEvent(eventSource: ${eventSource}, eventType: ${eventType}, networkId: ${networkId}, sourceId: ${sourceId}, destinationId: ${destinationId}, messageArgs: ${messageArgs})")
    switch(eventType){
        case "UPB_GOTO":
            def level = messageArgs[0]
            def rate = messageArgs[1]
            def channel = messageArgs[2]
            def deviceId = buildDeviceNetworkId(networkId, destinationId, channel)
            def device = getChildDevice(deviceId)
            if (device == null) {
                logWarn("No device found for ${deviceId}")
            } else {
                try {
                    device.handleGotoEvent(eventSource, eventType, networkId, sourceId, destinationId, level, rate, channel)
                } catch (Exception e) {
                    logWarn("Failed to call handleGotoEvent on ${deviceId}: ${e.message}")
                }
            }
            break;
        case "UPB_DEVICE_STATE":
            messageArgs.eachWithIndex { level, channel ->
                channel = channel + 1
                // Device report needs to be routed to the source, the destination is broadcasted
                def deviceId = buildDeviceNetworkId(networkId, sourceId, channel)
                def device = getChildDevice(deviceId)
                if (device == null) {
                    logWarn("No device found for ${deviceId}")
                } else {
                    try {
                        device.handleDeviceStateReport(eventSource, eventType, networkId, destinationId, sourceId, messageArgs)
                    } catch (Exception e) {
                        logWarn("Failed to call handleDeviceStateReport on ${deviceId}: ${e.message}")
                    }
                }
            }
            break;
        default:
            logWarn("Unhandled event eventType:${eventType}")
            break;
    }
}