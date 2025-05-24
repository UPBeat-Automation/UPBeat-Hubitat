/*
 * Hubitat Library: UPBeatLogger
 * Description: Logger library used by Apps and Devices
 * Copyright: 2025 UPBeat Automation
 * Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
 * Author: UPBeat Automation
 */
import groovy.transform.Field
import java.util.regex.Pattern

library (
        name: "UPBeatLogger",
        namespace: "UPBeat",
        category: "Convenience",
        description: "Logger library used by Apps and Devices",
        author: "UPBeat Automation"
)

@Field static final Map LOG_LEVELS = [0:"Off", 1:"Error", 2:"Warn", 3:"Info", 4:"Debug", 5:"Trace"]
@Field static final int LOG_DEFAULT_LEVEL = 0
@Field static final Pattern HHU_PATTERN = Pattern.compile('%([-#+ 0]*)(\\d*)(\\.\\d*)?hhu')

// Formats strings, handles %hhu for unsigned bytes, and byte arrays printed as hex
String sprintf(String format, Object... args) {
    def processedArgs = args.collect { arg ->
        if (arg instanceof Byte) {
            arg & 0xFF // Convert single Byte to unsigned (0-255)
        } else if (arg instanceof byte[]) {
            // Convert byte[] to comma-separated 0x%02X values
            ((byte[])arg).collect { String.format("0x%02X", it & 0xFF) }.join(", ")
        } else {
            arg
        }
    }
    def modifiedFormat = format.replaceAll(HHU_PATTERN, '%$1$2$3d')
    String.format(modifiedFormat, processedArgs as Object[])
}

void logError(String format, Object... args) {
    if (logLevel.toInteger() >= 1) {
        log.error(args ? sprintf(format, args) : format)
    }
}

void logWarn(String format, Object... args) {
    if (logLevel.toInteger() >= 2) {
        log.warn(args ? sprintf(format, args) : format)
    }
}

void logInfo(String format, Object... args) {
    if (logLevel.toInteger() >= 3) {
        log.info(args ? sprintf(format, args) : format)
    }
}

void logDebug(String format, Object... args) {
    if (logLevel.toInteger() >= 4) {
        log.debug args ? sprintf(format, args) : format
    }
}

void logTrace(String format, Object... args) {
    if (logLevel.toInteger() >= 5) {
        log.trace args ? sprintf(format, args) : format
    }
}