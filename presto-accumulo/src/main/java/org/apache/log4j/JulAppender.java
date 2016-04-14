/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.log4j;

import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import java.util.logging.LogRecord;

import static java.lang.String.format;

/**
 * We are unable to completely remove the log4j dependency due to ZooKeeper, which explicitly requires
 * an internal class of the log4j package and is unavailable in the slf4j-over-log4j artifact.
 * <p>
 * This JUL appender is a workaround for this issue, appending all log4j events to JUL.
 */
public class JulAppender
        extends AppenderSkeleton
{
    /**
     * Append a log event at the appropriate JUL level, depending on the log4j level.
     */
    @Override
    protected void append(LoggingEvent loggingEvent)
    {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(loggingEvent.getLoggerName());
        if (logger == null) {
            LogLog.warn(format("Cannot obtain JUL %s. Verify that this appender is used while an appropriate LogManager is active.", loggingEvent.getLoggerName()));
            return;
        }

        Level level = loggingEvent.getLevel();
        java.util.logging.Level julLevel = convertLog4jLevel(level);

        LogRecord record = new LogRecord(julLevel, loggingEvent.getRenderedMessage());
        record.setMillis(loggingEvent.getTimeStamp());
        LocationInfo location = loggingEvent.getLocationInformation();
        if (location != null) {
            record.setSourceClassName(location.getClassName());
            record.setSourceMethodName(location.getMethodName());
        }

        logger.log(record);
    }

    @Override
    public boolean requiresLayout()
    {
        return true;
    }

    @Override
    public void close() {}

    private static java.util.logging.Level convertLog4jLevel(Level log4jLevel)
    {
        if (log4jLevel.equals(Level.TRACE)) {
            return java.util.logging.Level.FINEST;
        }

        if (log4jLevel.equals(Level.DEBUG)) {
            return java.util.logging.Level.FINER;
        }

        if (log4jLevel.equals(Level.INFO)) {
            return java.util.logging.Level.INFO;
        }

        if (log4jLevel.equals(Level.WARN)) {
            return java.util.logging.Level.WARNING;
        }

        if (log4jLevel.equals(Level.ERROR)) {
            return java.util.logging.Level.SEVERE;
        }

        if (log4jLevel.equals(Level.FATAL)) {
            return java.util.logging.Level.SEVERE;
        }

        if (log4jLevel.equals(Level.ALL)) {
            return java.util.logging.Level.ALL;
        }

        if (log4jLevel.equals(Level.OFF)) {
            return java.util.logging.Level.OFF;
        }

        return java.util.logging.Level.FINE;
    }
}
