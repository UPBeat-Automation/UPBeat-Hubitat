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