/*
* Hubitat Library: UPBeatLogger
* Description: Logger library used by Apps and Devices
* Copyright: 2025 UPBeat Automation
* Licensed: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License
* Author: UPBeat Automation
*/
import groovy.transform.Field

library (
        name: "UPBeatLogger",
        namespace: "UPBeat",
        category: "Convenience",
        description: "Logger library used by Apps and Devices",
        author: "UPBeat Automation"
)

@Field static final Map LOG_LEVELS = [0:"Off", 1:"Error", 2:"Warn", 3:"Info", 4:"Debug", 5:"Trace"]
@Field static final int LOG_DEFAULT_LEVEL = 0

void logError(message) {
    if (logLevel?.toInteger()>=1) log.error message
}
void logWarn(message) {
    if (logLevel?.toInteger()>=2) log.warn message
}
void logInfo(message) {
    if (logLevel?.toInteger()>=3) log.info message
}
void logDebug(message) {
    if (logLevel?.toInteger()>=4) log.debug message
}
void logTrace(message) {
    if (logLevel?.toInteger()>=5) log.trace message
}


static Map createLogger(log) {
        def loggerMap = [
            log: null,
            logLevel: LOG_DEFAULT_LEVEL,
            
            setLogger: { logger ->
                delegate.log = logger
            },

            getLogger: {
                return delegate.log
            },
            
            setLogLevel: { int level ->
                delegate.logLevel = level
            },

            getLogLevel: {
                return delegate.logLevel
            },

            logError: { String message ->
                if (delegate.getLogLevel() >= 1) {
                    delegate.log.error message
                }
            },

            logWarn: { String message ->
                if (delegate.getLogLevel() >= 2) {
                    delegate.log.warn message
                }
            },

            logInfo: { String message ->
                if (delegate.getLogLevel() >= 3) {
                    delegate.log.info message
                }
            },

            logDebug: { String message ->
                if (delegate.getLogLevel() >= 4) {
                    delegate.log.debug message
                }
            },

            logTrace: { String message ->
                if (delegate.getLogLevel() >= 5) {
                    delegate.log.trace message
                }
            }
        ]

        loggerMap.each { key, value ->
            if (value instanceof Closure) {
                value.delegate = loggerMap
                value.resolveStrategy = Closure.DELEGATE_FIRST
            }
        }

        return loggerMap
}
