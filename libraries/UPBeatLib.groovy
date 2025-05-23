/*
* Hubitat Library: UPBeatLib
* Description: Universal Powerline Bus Helper Library for Hubitat
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
library(
        name: "UPBeatLib",
        namespace: "UPBeat",
        author: "UPBeat Automation",
        description: "Helper functions for UPB device and scene network ID encoding/decoding and packet sending",
        category: "Utilities",
        importUrl: ""
)

def buildDeviceNetworkId(int networkId, int unitId, int channel) {
    logTrace "buildDeviceNetworkId()"
    // Validate inputs (0-255 for each field)
    if (networkId < 0 || networkId > 255) {
        throw new IllegalArgumentException("NetworkId must be 0-255, got: ${networkId}")
    }
    if (unitId < 1 || unitId > 250) {
        throw new IllegalArgumentException("UnitId must be 1-250, got: ${unitId}")
    }
    if (channel < 0 || channel > 255) {
        throw new IllegalArgumentException("Channel must be 0-255, got: ${channel}")
    }

    // Build the ID as a hexadecimal string: PT:NET:UID:CH (e.g., 01872A00 for packetType=1)
    String deviceNetworkId = String.format("UPBeat_%02X%02X%02X", networkId, unitId, channel)

    // Log the components for debugging
    logDebug "DeviceNetworkId: ${deviceNetworkId} (NetworkId=${networkId}, UnitId=${unitId}, Channel=${channel})"

    return deviceNetworkId
}

def buildSceneNetworkId(int networkId, int linkId) {
    logTrace "buildSceneNetworkId()"
    // Validate inputs (0-255 for each field)
    if (networkId < 0 || networkId > 255) {
        throw new IllegalArgumentException("NetworkId must be 0-255, got: ${networkId}")
    }
    if (linkId < 1 || linkId > 250) {
        throw new IllegalArgumentException("LinkId must be 1-250, got: ${linkId}")
    }

    // Build the ID as a hexadecimal string: PT:NET:LID (e.g., 000A32 for packetType=0)
    String sceneNetworkId = String.format("UPBeat_%02X%02X", networkId, linkId)

    // Log the components for debugging
    logDebug "SceneNetworkId: ${sceneNetworkId} (NetworkId=${networkId}, LinkId=${linkId})"

    return sceneNetworkId
}

def decodeDeviceNetworkId(String deviceNetworkId) {
    logTrace "decodeDeviceNetworkId()"
    // Expected format: 8 characters (e.g., "01872A00")
    if (deviceNetworkId.length() != 8) {
        throw new IllegalArgumentException("DeviceNetworkId must be 8 characters, got: ${deviceNetworkId}")
    }

    try {
        int packetType = Integer.parseInt(deviceNetworkId.substring(0, 2), 16)
        int networkId = Integer.parseInt(deviceNetworkId.substring(2, 4), 16)
        int unitId = Integer.parseInt(deviceNetworkId.substring(4, 6), 16)
        int channel = Integer.parseInt(deviceNetworkId.substring(6, 8), 16)

        if (packetType != 1) {
            throw new IllegalArgumentException("DeviceNetworkId must have packetType=1, got: ${packetType}")
        }

        return [
                packetType: packetType,
                networkId: networkId,
                unitId: unitId,
                channel: channel
        ]
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid DeviceNetworkId format: ${deviceNetworkId}", e)
    }
}

def decodeSceneNetworkId(String sceneNetworkId) {
    logTrace "decodeSceneNetworkId()"
    // Expected format: 6 characters (e.g., "008720")
    if (sceneNetworkId.length() != 6) {
        throw new IllegalArgumentException("SceneNetworkId must be 6 characters, got: ${sceneNetworkId}")
    }

    try {
        int packetType = Integer.parseInt(sceneNetworkId.substring(0, 2), 16)
        int networkId = Integer.parseInt(sceneNetworkId.substring(2, 4), 16)
        int linkId = Integer.parseInt(sceneNetworkId.substring(4, 6), 16)

        if (packetType != 0) {
            throw new IllegalArgumentException("SceneNetworkId must have packetType=0, got: ${packetType}")
        }

        return [
                packetType: packetType,
                networkId: networkId,
                linkId: linkId
        ]
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid SceneNetworkId format: ${sceneNetworkId}", e)
    }
}

def sendUpbReport(String networkId, String reportMdid, String sourceId, String... args) {
    logTrace "sendUpbReport(networkId=${networkId}, reportMdid=${reportMdid}, sourceId=${sourceId}, args=${args})"

    // Decode the network ID to determine if it's a device or scene
    Map decodedId
    boolean isDevice
    try {
        if (networkId.length() == 8) {
            decodedId = decodeDeviceNetworkId(networkId)
            isDevice = true
        } else if (networkId.length() == 6) {
            decodedId = decodeSceneNetworkId(networkId)
            isDevice = false
        } else {
            throw new IllegalArgumentException("Invalid networkId length: ${networkId}")
        }
    } catch (Exception e) {
        logError "Failed to decode networkId: ${e.message}"
        return
    }

    // Extract fields
    int nid = decodedId.networkId
    int did = isDevice ? decodedId.unitId : decodedId.linkId
    int sid = Integer.parseInt(sourceId, 16) // Source ID (Unit ID of sender)
    int mdid = Integer.parseInt(reportMdid, 16)

    // Validate source ID
    if (sid < 1 || sid > 250) {
        logError "SourceId must be 1-250, got: ${sid}"
        return
    }

    // Construct the packet
    // Control Word: LEN includes header (5 bytes) + MDID (1 byte) + args + CHK (1 byte)
    int packetLength = 5 + 1 + args.length + 1 // Header + MDID + args + CHK
    if (packetLength < 6 || packetLength > 24) {
        logError "Packet length must be 6-24 bytes, got: ${packetLength}"
        return
    }
    // CTL: LNK bit (1 for scene, 0 for device), LEN, no repeats, no ACK
    int ctlHigh = (isDevice ? 0x00 : 0x80) | ((packetLength >> 2) & 0x1F) // LNK bit + high bits of LEN
    int ctlLow = (packetLength & 0x03) << 6 // Low bits of LEN, no REPRQ, ACKRQ, CNT, SEQ

    // Build the packet bytes (excluding preamble)
    List<Integer> packetBytes = []
    packetBytes << ctlHigh
    packetBytes << ctlLow
    packetBytes << nid
    packetBytes << did
    packetBytes << sid
    packetBytes << mdid
    args.each { arg ->
        packetBytes << Integer.parseInt(arg, 16)
    }

    // Compute checksum: 2's complement of the sum of header and message bytes
    int sum = packetBytes.sum()
    int chk = (-sum & 0xFF) // 2's complement, truncated to 8 bits
    packetBytes << chk

    // Convert to byte array
    byte[] packetData = new byte[packetBytes.size()]
    packetBytes.eachWithIndex { value, index ->
        packetData[index] = (byte)(value & 0xFF)
    }

    // Log the packet
    String packetHex = packetData.collect { String.format("%02X", it & 0xFF) }.join()
    logDebug "Sending UPB Report Packet: ${packetHex}"

    // Send the packet (requires a parent app with sendPimMessage)
    try {
        if (parent?.sendPimMessage(packetData)) {
            logDebug "UPB Report Packet sent successfully: ${packetHex}"
        } else {
            logWarn "Failed to send UPB Report Packet: ${packetHex}"
        }
    } catch (Exception e) {
        logError "Error sending UPB Report Packet: ${e.message}"
    }
}

@Field static final int UPE_FILE_VERSION = 5

@Field static final Map UPB_MANUFACTURER_ID_MAP = [
        "0": "OEM",
        "1": "PCS",
        "2": "MDManufacturing",
        "3": "WebMountainTech",
        "4": "SimplyAutomated",
        "5": "HAI",
        "10": "RCS",
        "90": "OEM90",
        "91": "OEM91",
        "92": "OEM92",
        "93": "OEM93",
        "94": "OEM94",
        "95": "OEM95",
        "96": "OEM96",
        "97": "OEM97",
        "98": "OEM98",
        "99": "OEM99"]

@Field static final Map KIND_MAP = [
        "0": "Other",
        "1": "Keypad",
        "2": "Switch",
        "3": "Module",
        "4": "Input Module",
        "5": "Input-Output Module",
        "6": "Vacuum Power Module",
        "7": "Vacuum Handle Controller",
        "8": "Thermostat"]

@Field static final Map PACKET_TYPE_MAP = [
        "0": "Direct",
        "1": "Link"]

@Field static final Map PCS_PRODUCTS = [
        "1": "(WS1) Wall Switch - 1 Channel Switch",
        "2": "(WS1R) Wall Switch – Relay Switch",
        "3": "(WMC6) Wall Mount Controller - 6 Button Keypad",
        "4": "(WMC8) Wall Mount Controller - 8 Button Keypad",
        "6": "(OCM2) Output Control Module - 2 Channel Module",
        "7": "(LCM1) Load Control Module 1 Module",
        "9": "(LM1) Lamp Module - 1 Channel Module",
        "10": "(LM2) Lamp Module – 2 Channel Module",
        "11": "(ICM2) Input Control Module - 2 Channel Input",
        "13": "(DTC6) Desktop Controller - 6 Button Keypad",
        "14": "(DTC8) Desktop Controller - 8 Button Keypad",
        "15": "(AM1) Appliance Module - 1 Channel Module",
        "25": "(LSM) Load Shedding Module Module",
        "24": "(WS1E) Wall Switch - Electronic Low Voltage Switch",
        "36": "(DCM) Doorbell Control Module Input",
        "37": "(TCM) Telephone Control Module Input",
        "60": "(FMD2) Fixture Module – Dimmer Module",
        "61": "(FMR) Fixture Module - Relay Module",
        "62": "(WS2D) LED Wall Switch Switch",
        "65": "(KPC6) Controller – 6 Button Keypad",
        "66": "(KPC8) Controller – 8 Button Keypad"]

@Field static final Map HAI_PRODUCTS = [
        "1": "35A00-1 600W Dimming Switch",
        "2": "35A00-2 1000W Dimming Switch",
        "16": "35A00-3 600W Non-Dimming Switch",
        "17": "35A00-4 1000W Non-Dimming Switch",
        "18": "40A00-1 15A Relay Switch",
        "3": "55A00-1 1000W Dimming Switch",
        "4": "55A00-2 1500W Dimming Switch",
        "5": "55A00-3 2400W Dimming Switch",
        "32": "59A00-1 300W Lamp Module",
        "48": "60A00-1 15A Appliance Module",
        "80": "38A00-1 6-Button Room Controller Keypad",
        "81": "HLCK6 6-Button Room Controller Keypad",
        "96": "38A00-2 8-Button House Controller Keypad"]


@Field static final Map SAI_WMT_OEM_PRODUCTS = [
        "1": "UML Lamp Module Module",
        "5": "UMA Appliance Module Module",
        "7": "UFR Fixture Relay / URD Receptacle Switch or Module",
        "9": "UMA Appliance Module – Timer Module",
        "10": "UFD Fixture Dimmer Switch or Module",
        "12": "UML Lamp Module – Timer Module",
        "13": "UFR Fixture / URD Receptacle – Timer Switch or Module",
        "14": "UFD Fixture Dimmer – Timer Switch or Module",

        "15": "UCT Tabletop Controller Keypad",
        "20": "USM1 Switch Motorized Switch",
        "22": "US1 / US2 Series Dimming Switch Switch",
        "26": "UCQ / UCQT Quad Output Module Module",
        "27": "US4 Series Quad Dimming Switch Switch",
        "28": "US1-40 Series Dimming Switch Switch",
        "29": "US2-40 Series Dimming Switch Switch",
        "30": "Serial PIM",
        "31": "USB PIM",
        "32": "Ethernet PIM",
        "33": "Signal Quality Monitoring Unit",
        "34": "US1-40 Series Dimming Switch – Timer Switch",
        "36": "UCQTX Quad Output Module Module",
        "40": "UMI-32 3-Input / 2-Output Module Input-Output Module",
        "41": "Input Module",
        "43": "Sprinker Controller",
        "44": "USM1R Switch",
        "45": "USM2R Switch",
        "50": "UQC",
        "51": "UQC 40",
        "52": "UQC F",
        "62": "US22-40T Series Dimming Switch Switch",
        "201": "Lamp Module (UML-E) Module",
        "205": "Appliance Module (UMA-E) Module",
        "222": "Retail Dimming Switch (RS101) Switch",
        "240": "Retail I/O 32 Module Input-Output Module"]

//"88": "CLSW-01 Classic series single dimmer wall switch",
//"89": "CL6-01 Classic series wall mount 6 button controller Keypad",

@Field static final Map MD_PRODUCTS = [
        "32": "(VHC) Vacuum Handle Controller",
        "33": "(VPM) Vacuum Power Module",
        "35": "(VIM) Vacuum Input Module",
        "36": "(DSM) Doorbell Sense Module",
        "37": "(TSM) Telephone Sense Module"]

@Field static final Map UPB_ACTIONS = [
        "0": "Goto Off",
        "1": "Goto On",
        "2": "Fade Down",
        "3": "Fade Up",
        "4": "Fade Stop",
        "5": "Deactivate",
        "6": "Activate",
        "7": "Snap Off",
        "8": "Snap On",
        "9": "Quick Off",
        "10": "Quick On",
        "11": "Slow Off",
        "12": "Slow On",
        "13": "Blink",
        "14": "Null",
        "15": "No Command"]

@Field static final Map minFieldCounts = [
        '0': 6, '1': 1, '2': 3, '3': 14, '4': 6, '5': 14, '6': 15, '7': 10,
        '8': 5, '9': 5, '10': 12, '11': 12, '12': 3, '13': 7, '14': 10,
        '18': 3, '19': 3
]

@Field static final Map expectedRecordTypes = [
        '1': ['6', '13', '17'], '2': ['4', '5'], '3': ['4', '5'], '4': ['7'],
        '5': ['4', '5', '7'], '6': ['4', '5'], '7': ['4', '9'], '8': ['14'],
        '9': ['15'], '10': ['16', '5', '6'], '0': []
]

static List<List<String>> parse_csv(byte[] csvBytes) {
    def rows = []
    def currentRow = []
    def field = ''
    def insideQuotes = false
    def multilineField = false

    csvBytes.each { b ->
        def ch = (char) b
        if (ch == '"') {
            if (insideQuotes) {
                insideQuotes = false
                if (field.endsWith('"')) field += '"'
                else { field += '"'; multilineField = true }
            } else {
                insideQuotes = true
            }
        } else if (ch == ',' && !insideQuotes) {
            if (multilineField) field += '\n'
            else { currentRow.add(field); field = '' }
        } else if (ch == '\n' && !insideQuotes) {
            if (multilineField) {
                field += '\n'
                multilineField = false
                currentRow.add(field)
                field = ''
            } else {
                currentRow.add(field)
                rows.add(currentRow)
                currentRow = []
                field = ''
            }
        } else {
            field += ch
        }
    }

    if (field) currentRow.add(field)
    if (currentRow) rows.add(currentRow)
    return rows
}

def processUpeFile(String userInput) {
    logDebug "processUpeFile()"

    def rows = parse_csv(userInput.getBytes())
    def data = [
            'systemInfo': [:], 'installer': [:], 'customer': [:],
            'links': [], 'modules': [], 'roomIcons': []
    ]

    def current_device = [:]

    for (int i = 0; i < rows.size(); i++) {
        def row = rows[i]
        def recordType = row[0]

        if (recordType in minFieldCounts && row.size() < minFieldCounts[recordType]) {
            logDebug("Warning: Invalid record ${recordType}: too few fields (${row.size()})")
            continue
        }

        switch (recordType) {
            case "0": // BOF
                data['systemInfo'] = [
                        'version': row[1].toInteger(),
                        'totalModules': row[2].toInteger(),
                        'totalLinks': row[3].toInteger(),
                        'networkId': row[4].toInteger(),
                        'networkPass': row[5].toInteger()
                ]
                break
            case "1": // EOF
                logDebug("End of Configuration")
                break
            case "2": // Link
                data['links'].add(['linkId': row[1].toInteger(), 'name': row[2]])
                break
            case "3": // Module
                logDebug(row)
                current_device = ["moduleId": row[1].toInteger(),
                                  "networkId": row[2].toInteger(),
                                  "productId": row[3].toInteger(),
                                  "manufacturerId": row[4].toInteger(),
                                  "firmwareMajorVersion": row[5].toInteger(),
                                  "firmwareMinorVersion": row[6].toInteger(),
                                  "deviceType": row[7].toInteger(),
                                  "channels": row[8].toInteger(),
                                  "transmitComponents": row[9].toInteger(),
                                  "receiveComponents": row[10].toInteger(),
                                  "roomName": row[11],
                                  "deviceName": row[12],
                                  "packetType": row[13].toInteger()]
                data['modules'].add(current_device)
                break
            case "4": // Preset
                current_device.putIfAbsent('presetInfo', [])
                current_device['presetInfo'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "linkId": row[4].toInteger(),
                        "presetDimLevel": row[5].toInteger(),
                        "presetDimFadeRate": row[6].toInteger()])
                break
            case "5": // Rocker
                current_device.putIfAbsent('rockers', [])
                current_device['rockers'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "topRockerTransmitLinkId": row[4].toInteger(),
                        "topRockerSingleClickAction": row[5].toInteger(),
                        "topRockerDoubleClickAction": row[6].toInteger(),
                        "topRockerHoldAction": row[7].toInteger(),
                        "topRockerReleaseAction": row[8].toInteger(),
                        "bottomRockerTransmitLinkId": row[9].toInteger(),
                        "bottomRockerSingleClickAction": row[10].toInteger(),
                        "bottomRockerDoubleClickAction": row[11].toInteger(),
                        "bottomRockerHoldAction": row[12].toInteger(),
                        "bottomRockerReleaseAction": row[13].toInteger()
                ])
                break
            case "6": // Button
                current_device.putIfAbsent('buttons', [])
                current_device['buttons'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "buttonLinkId": row[4].toInteger(),
                        "singleClickAction": row[5].toInteger(),
                        "doubleClickAction": row[6].toInteger(),
                        "holdAction": row[7].toInteger(),
                        "releaseAction": row[8].toInteger(),
                        "singleClickToggleAction": row[9].toInteger(),
                        "doubleClickToggleAction": row[10].toInteger(),
                        "holdToggleAction": row[11].toInteger(),
                        "releaseToggleAction": row[12].toInteger(),
                        "indicatorLink": row[13].toInteger(),
                        "indicatorByte": row[14].toInteger()
                ])
                break
            case "7": // Input
                current_device.putIfAbsent('inputs', [])
                current_device['inputs'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "openLinkId": row[4].toInteger(),
                        "openCommandId": row[5].toInteger(),
                        "openToggleCommandId": row[6].toInteger(),
                        "closeLinkId": row[7].toInteger(),
                        "closeCommandId": row[8].toInteger(),
                        "closeToggleCommandId": row[9].toInteger()
                ])
                break
            case "8": // Channel Info
                current_device.putIfAbsent('channelInfo', [])
                current_device['channelInfo'].add([
                        "channelId": row[1].toInteger(),
                        "dimEnabled": row[3].toInteger(),
                        "defaultFadeRate": row[4].toInteger()
                ])
                break
            case "9": // VHC
                current_device.putIfAbsent('vhcs', [])
                current_device['vhcs'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "transmitCommand": row[4].toInteger()
                ])
                break
            case "10": // Installer Info
                data['installer'] = [
                        "company": row[1] ?: "",
                        "name": row[2] ?: "",
                        "address": row[3] ?: "",
                        "city": row[4] ?: "",
                        "state": row[5] ?: "",
                        "zip": row[6] ?: "",
                        "phone": row[7] ?: "",
                        "email": row[8] ?: "",
                        "fax": row[9] ?: "",
                        "pager": row[10] ?: "",
                        "web": row[11] ?: ""
                ]
                break
            case "11": // Customer
                data['customer'] = [
                        "company": row[1] ?: "",
                        "name": row[2] ?: "",
                        "address": row[3] ?: "",
                        "city": row[4] ?: "",
                        "state": row[5] ?: "",
                        "zip": row[6] ?: "",
                        "phone": row[7] ?: "",
                        "email": row[8] ?: "",
                        "fax": row[9] ?: "",
                        "pager": row[10] ?: "",
                        "web": row[11] ?: ""
                ]
                break
            case "12": // Device Memory
                logDebug("Memory (${current_device}): ${row[1..-1]}")
                current_device.putIfAbsent('memory', [])
                current_device['memory'].add([
                        "address": row[1],
                        "data": row[2..-1].join(',')
                ])
                break
            case "13": // Keypad Indicator
                current_device.putIfAbsent('receive', [])
                current_device['receive'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "linkId": row[4].toInteger(),
                        "mask1": row[5].toInteger(),
                        "mask2": row[6].toInteger()
                ])
                break
            case "14": // Thermostat
                current_device.putIfAbsent('thermostats', [])
                current_device['thermostats'].add([
                        "channelId": row[1].toInteger(),
                        "componentId": row[2].toInteger(),
                        "firmwareVersion": row[4],
                        "wduVersion": row[5],
                        "units": row[6].toInteger(),
                        "inhibitLink": row[7].toInteger(),
                        "linkBase": row[8].toInteger(),
                        "setpointDelta": row[9].toInteger()
                ])
                break
            case "18": // Room Icon
                //logDebug("Room Name: ${row[1]}")
                //logDebug("Icon Name: ${row[2]}")
                //data['roomIcons'].add(["roomName": row[1], "iconName": row[2]])
                break
            case "19": // Device Icon
                //logDebug("Module ID: ${row[1]}")
                //logDebug("Icon Name: ${row[2]}")
                //data['modules'][row[1]].putIfAbsent('deviceIcon', [])
                //data['modules'][row[1]]['deviceIcon'] = ["iconName": row[2..-1].join(',')]
                break
            default:
                logDebug("Unknown Data: ${row[0]}")
                break
        }

        if (data['systemInfo']['version'] && data['systemInfo']['version'] != UPE_FILE_VERSION) {
            logDebug "UPE file version is ${data['systemInfo']['version']} must be ${UPE_FILE_VERSION}"
            return data
        }
    }
    return data
}
