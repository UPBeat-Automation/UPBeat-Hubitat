/*
* Hubitat App: UPBeat App
* Description: Hubitat App for Univeral Powerline Bus Support
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
import groovy.json.JsonBuilder
import hubitat.helper.HexUtils
import groovy.transform.Field
import java.util.concurrent.ConcurrentHashMap
import java.security.MessageDigest
import java.util.UUID

#include UPBeat.UPBeatLogger
#include UPBeat.UPBeatLib

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
        "dimming_switch": [
                displayName: "UPB Dimming Switch",
                driverName: "UPB Dimming Switch",
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
                input name: "deviceName", type: "text", title: "Device Name", required: true, submitOnChange: true
                input name: "voiceName", type: "text", title: "Voice Name", required: false, submitOnChange: true
                input name: "networkId", type: "number", title: "Network ID", required: true, range: "0..255", submitOnChange: true

                // Dynamically render inputs based on device type
                DEVICE_TYPES[settings.deviceType].requiredInputs.each { inputConfig ->
                    input(inputConfig + [submitOnChange: true])
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
        logError "Failed to create device: ${e.message}"
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
        logError "Failed to retrieve newly created device with network ID ${deviceNetworkId}"
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
        section("Devices") {
            if (app.getInstallationState() == "COMPLETE") {
                href(name: "manualAddHref", title: "Manually Add Device", page: "addDevicePage", description: "Add a device manually")
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
@Field static ConcurrentHashMap currentLoadedDevice = new ConcurrentHashMap()
@Field static String pimDeviceId = "UPBeat_PIM"

/***************************************************************************
 * Core App Functions
 ***************************************************************************/
void installed() {
    logTrace "installed()"
    updated()
}

void uninstalled() {
    logTrace "uninstalled()"
    unsubscribe()
}

void updated() {
    logTrace "updated()"
    unsubscribe()
    def pim = getPimDevice()
    if (pim) {
        subscribe(pim, "linkEvent", "handlePimLinkEvent")
        subscribe(pim, "deviceState", "handlePimDeviceState")
        logDebug "Subscribed to linkEvent and deviceState from PIM ${pim.deviceNetworkId}"
    }
}

def getPimDevice()
{
    logTrace "getPimDevice()"

    def pim = getChildDevice(pimDeviceId)

    if (pim == null) {
        logDebug "Creating PIM device"
        pim = addChildDevice("UPBeat", "UPB Powerline Interface Module", pimDeviceId, [name: "UPB Powerline Interface Module"])
    }

    return pim
}

/*
def updateDeviceId(device, String newDeviceNetworkId) {
    try {
        def childDevice = getChildDevice(newDeviceNetworkId)
        if (!childDevice) {
            logError "Child device with network ID ${childDeviceNetworkId} not found"
            return false
        }

        def newDeviceNetworkId = childDevice.deriveNetworkId()

        if (!newDeviceNetworkId || newDeviceNetworkId.trim() == "") {
            logError "Derived Network ID is invalid for ${childDevice.displayName}"
            return false
        }

        def conflict = getChildDevice(newDeviceNetworkId)
        if (!conflict) {
            childDevice.deviceNetworkId = newDeviceNetworkId
            logDebug "Device ID for ${childDevice.displayName} updated to ${newDeviceNetworkId}"
        } else {
            logError "Device ID ${conflict.displayName} conflicts with ${newDeviceNetworkId} (requested by ${childDeviceNetworkId})"
            return false
        }
    } catch (Exception e) {
        logError "An error occurred while updating the device ID for ${childDeviceNetworkId}: ${e.message}"
        return false
    }
    return true
}
*/

void appButtonHandler(button) {
    switch(button) {
        case "addDeviceBtn":
            logTrace("createDevice")
            // Validate inputs based on device type
            if (!settings.deviceType || !settings.deviceName || !settings.networkId) {
                logError "Device Type, Device Name, and Network ID are required."
                return
            }
            if (settings.deviceType != "UPB Scene") {
                if (!settings.deviceId || !settings.channelId) {
                    logError "Device ID and Channel ID are required for ${settings.deviceType} devices."
                    return
                }
            } else {
                if (!settings.linkId) {
                    logError "Link ID is required for UPB Scene devices."
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
                logError "A device with Network ID ${settings.networkId}, Device/Link ID ${settings.deviceType == 'UPB Scene' ? settings.linkId : settings.deviceId}, and Channel ID ${settings.channelId ?: 'N/A'} already exists."
                return
            }

            // Create the device
            def childDevice
            try {
                childDevice = addChildDevice("UPBeat", settings.deviceType, deviceNetworkId, [name: settings.deviceName, label: settings.voiceName ?: settings.deviceName])
            } catch (Exception e) {
                logError "Failed to create device: ${e.message}"
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
    }
}

/***************************************************************************
 * App Helper Functions
 ***************************************************************************/
private String makeUri(String extraPath) {
    logTrace "makeUri()"
    return getFullLocalApiServerUrl() + extraPath + "?access_token=${state.accessToken}"
}

String getHubUrl() {
    def localIP = location.hub.localIP
    def hubUrl = "https://${localIP}"
    return hubUrl
}

void updatePIMDevice(String ipAddress, int portNumber) {
    logTrace "updatePIMDevice()"
    def pim = getPimDevice()
    // Set the device IP
    device.updateSetting("ipAddress", [value: ipAddress, type: "text"])
    device.updateSetting("portNumber", [value: portNumber, type: "number"])
    device.updated()
}

void deleteAllDevices() {
    logTrace "deleteAllDevices()"
    def devices = app.getChildDevices()
    // Delete all child devices
    devices.each { device ->
        logDebug "Deleting ${device.deviceNetworkId}"
        deleteChildDevice(device.deviceNetworkId)
    }
}

void eventHandler(evt) {
    logTrace "eventHandler()"
    logDebug evt
}

void getDevices() {
    logTrace "getDevices()"
}

Map getCurrentLoaded() {
    logTrace "getCurrentLoaded()"
    return currentLoadedDevice
}

/***************************************************************************
 * Web Service Handlers for Configuration Application
 ***************************************************************************/
void handleStatus() {
    logTrace "handleStatus()"

    def data = [
            message: "UPBeat is alive an well."
    ]

    // Using JsonBuilder to convert the data map to a JSON string
    def json = new JsonBuilder(data).toPrettyString()

    render contentType: "application/json", data: json, status: 200
}

void handleAddDevice() {
    logTrace "handleAddDevice()"

    def postData = request.JSON

    logDebug "Received POST data: ${postData}"

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
    logTrace "handleAddScene()"

    def postData = request.JSON

    logDebug "Received POST data: ${postData}"

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
    logTrace "handleUpdatePowerlineInterface()"

    def postData = request.JSON

    logDebug "Received POST data: ${postData}"

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
private static byte checksum(byte[] data) {
    byte sum = data.sum()
    return (~sum + 1)
}

byte[] buildSceneActivateCommand(Integer networkId, Integer linkId, Integer sourceId) {
    logTrace "buildSceneActivateCommand()"
    logDebug "Link ID: ${linkId}"
    logDebug "Source ID: ${sourceId}"

    def packet = new ByteArrayOutputStream()
    packet.write([0x87, 0x04] as byte[]) // Control Word
    packet.write(networkId) // Network ID
    packet.write(linkId) // Link ID
    packet.write(sourceId) // Source ID (PIM)
    packet.write(0x20) // MDID (Scene Activate)

    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: ${(short) sum & 0xFF}"
    packet.write(sum)

    String packet_text_hex = HexUtils.byteArrayToHexString(packet.toByteArray())

    logDebug "PIM Packet: ${packet_text_hex}"
    byte[] encoded_packet = packet_text_hex.getBytes()

    message = new ByteArrayOutputStream()
    message.write(0x14) // Transmit Byte
    message.write(encoded_packet) // UPB Message + Checksum
    message.write(0x0D) // EOL
    pim_bytes = message.toByteArray()

    logDebug "PIM Message Encoded: ${HexUtils.byteArrayToHexString(pim_bytes)}"
    return (pim_bytes)
}

byte[] buildSceneDeactivateCommand(Integer networkId, Integer linkId, Integer sourceId) {
    logTrace "buildSceneDeactivateCommand()"
    logDebug "Link ID: ${linkId}"
    logDebug "Source ID: ${sourceId}"

    def packet = new ByteArrayOutputStream()
    packet.write([0x87, 0x04] as byte[]) // Control Word
    packet.write(networkId) // Network ID
    packet.write(linkId) // Link ID
    packet.write(sourceId) // Source ID (PIM)
    packet.write(0x21) // MDID (Scene Deactivate)

    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: ${(short) sum & 0xFF}"
    packet.write(sum)

    String packet_text_hex = HexUtils.byteArrayToHexString(packet.toByteArray())

    logDebug "PIM Packet: ${packet_text_hex}"
    byte[] encoded_packet = packet_text_hex.getBytes()

    message = new ByteArrayOutputStream()
    message.write(0x14) // Transmit Byte
    message.write(encoded_packet) // UPB Message + Checksum
    message.write(0x0D) // EOL
    pim_bytes = message.toByteArray()

    logDebug "PIM Message Encoded: ${HexUtils.byteArrayToHexString(pim_bytes)}"
    return (pim_bytes)
}

byte[] buildGotoCommand(Integer networkId, Integer deviceId, Integer level, Integer duration, Integer channel) {
    logDebug "buildGotoCommand()"
    logDebug "Device ID: ${deviceId}"
    logDebug "Duration: ${duration}"
    logDebug "Level: ${level}"
    logDebug "Channel: ${channel}"

    // Validate inputs
    if (deviceId < 0 || deviceId > 255) {
        logError "Device ID ${deviceId} is out of range (0-255)"
        throw new IllegalArgumentException("Device ID must be between 0 and 255")
    }
    if (level < 0 || level > 100) {
        logError "Level ${level} is out of range (0-100)"
        throw new IllegalArgumentException("Level must be between 0 and 100")
    }
    if (duration < 0 || duration > 255) {
        logError "Duration ${duration} is out of range (0-255)"
        throw new IllegalArgumentException("Duration must be between 0 and 255")
    }
    if (channel < 0 || channel > 255) {
        logError "Channel ${channel} is out of range (0-255)"
        throw new IllegalArgumentException("Channel must be between 0 and 255")
    }
    if (networkId < 0 || networkId > 255) {
        logError "Network ID ${networkId} is out of range (0-255)"
        throw new IllegalArgumentException("Network ID must be between 0 and 255")
    }

    def packet = new ByteArrayOutputStream()
    packet.write([0x0a, 0x04] as byte[]) // Control Word
    packet.write(networkId) // Network ID
    packet.write(deviceId) // Device ID
    packet.write(0xFF) // Source ID (PIM)
    packet.write(0x22) // MDID (Goto)
    packet.write(level) // Level
    packet.write(duration) // Rate
    packet.write(channel) // Channel

    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: 0x${String.format('%02X', (short)sum & 0xFF)}"
    packet.write(sum)

    String packetTextHex = HexUtils.byteArrayToHexString(packet.toByteArray())

    logDebug "PIM Packet: ${packetTextHex}"
    byte[] encodedPacket = packetTextHex.getBytes()

    message = new ByteArrayOutputStream()
    message.write(0x14) // Transmit Byte
    message.write(encodedPacket) // UPB Message + Checksum
    message.write(0x0D) // EOL
    pimBytes = message.toByteArray()

    logDebug "PIM Message Encoded: ${HexUtils.byteArrayToHexString(pimBytes)}"
    return pimBytes
}

byte[] buildDeviceStateRequestCommand(Integer networkId, Integer deviceId) {
    // Validate inputs
    if (deviceId < 0 || deviceId > 255) {
        logError "Device ID ${deviceId} is out of range (0-255)"
        throw new IllegalArgumentException("Device ID must be between 0 and 255")
    }
    if (networkId < 0 || networkId > 255) {
        logError "Network ID ${networkId} is out of range (0-255)"
        throw new IllegalArgumentException("Network ID must be between 0 and 255")
    }

    def packet = new ByteArrayOutputStream()
    packet.write([0x07, 0x04] as byte[]) // Control Word
    packet.write(networkId) // Network ID
    packet.write(deviceId) // Device ID
    packet.write(0x00) // Source ID (PIM)
    packet.write(0x30) // MDID (Report State Command)
    //packet.write(channel) // Channel

    byte sum = checksum(packet.toByteArray()) // Returns a byte checksum
    logDebug "Checksum: 0x${String.format('%02X', (short)sum & 0xFF)}"
    packet.write(sum)

    String packetTextHex = HexUtils.byteArrayToHexString(packet.toByteArray())

    logDebug "PIM Packet: ${packetTextHex}"
    byte[] encodedPacket = packetTextHex.getBytes()

    message = new ByteArrayOutputStream()
    message.write(0x14) // Transmit Byte
    message.write(encodedPacket) // UPB Message + Checksum
    message.write(0x0D) // EOL
    pimBytes = message.toByteArray()

    logDebug "PIM Message Encoded: ${HexUtils.byteArrayToHexString(pimBytes)}"
    return pimBytes
}

/***************************************************************************
 * Custom App Functions
 ***************************************************************************/
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
                logInfo "Device ${deviceNetworkId} already exists"
            }
        } else {
            logInfo "Skipping ${deviceNetworkId} not enabled"
        }
    }
}

boolean sendPimMessage(byte[] bytes) {
    logTrace "sendPimMessage()"
    def pim = getPimDevice()
    return pim.transmitMessage(bytes)
}

def handlePimLinkEvent(evt) {
    logTrace "handleControlCommandEvent()"
    logDebug "Data: ${evt.value}"
    def eventData = new groovy.json.JsonSlurper().parseText(evt.value)
    def sceneId = buildSceneNetworkId(eventData.networkId, eventData.linkId)
    def device = getChildDevice(sceneId)
    if (device == null) {
        logWarn "No scene device found for ${sceneId}"
        return
    }
    try {
        device.handleLinkEvent(eventData.eventType, eventData.networkId, eventData.sourceId, eventData.linkId)
        logDebug "Dispatched handleLinkEvent to ${device.typeName} for ${eventData.eventType}"
    } catch (Exception e) {
        logWarn "Failed to call handleLinkEvent on ${sceneId}: ${e.message}"
    }
}

def handlePimDeviceState(evt) {
    logTrace "handleCoreReportEvent()"
    logDebug "Data: ${evt.value}"
    def eventData = new groovy.json.JsonSlurper().parseText(evt.value)
    def deviceId = eventData.destinationId == 0 ? buildDeviceNetworkId(eventData.networkId, eventData.sourceId, 1) : buildDeviceNetworkId(eventData.networkId, eventData.destinationId, 1)
    def device = getChildDevice(deviceId)
    if (device == null) {
        logWarn "No device found for ${deviceId}"
        return
    }
    try {
        // Broadcast packet needs to be routed to the device
        if(eventData.destinationId == 0)
            device.handleDeviceState(eventData.level, eventData.networkId, eventData.destinationId, eventData.sourceId, eventData.args)
        else
            device.handleDeviceState(eventData.level, eventData.networkId, eventData.sourceId, eventData.destinationId, eventData.args)
        logDebug "Dispatched handleDeviceState to ${device.typeName}"
    } catch (Exception e) {
        logWarn "Failed to call handleDeviceState on ${deviceId}: ${e.message}"
    }
}