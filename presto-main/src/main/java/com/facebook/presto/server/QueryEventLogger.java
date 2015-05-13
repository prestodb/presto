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
package com.facebook.presto.server;

import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.execution.QueryManagerConfig;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Throwables;
import io.airlift.event.client.AbstractEventClient;
import io.airlift.event.client.JsonEventSerializer;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class QueryEventLogger
        extends AbstractEventClient
{
    private final Logger logger = Logger.getLogger("QueryEventLogger");

    private final JsonFactory factory = new JsonFactory();
    private final JsonEventSerializer serializer = new JsonEventSerializer(QueryCompletionEvent.class);
    private final String completedQueriesLogFile;

    @Inject
    public QueryEventLogger(QueryManagerConfig queryManagerConfig)
    {
        this.completedQueriesLogFile = queryManagerConfig.getCompletedQueriesLogFile();
        if (completedQueriesLogFile != null) {
            try {
                FileHandler fh = new FileHandler(completedQueriesLogFile);
                fh.setFormatter(new LogFormatter());
                logger.setUseParentHandlers(false);
                logger.addHandler(fh);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    protected <T> void postEvent(T event)
            throws IOException
    {
        if (completedQueriesLogFile == null) {
            //discard event
            return;
        }

        //for now only hook into the query completion events
        if (event instanceof QueryCompletionEvent) {
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                try (JsonGenerator generator = factory.createGenerator(buffer, JsonEncoding.UTF8)) {
                    serializer.serialize(event, generator);
                    logger.info(buffer.toString());
                }
            }
        }
    }
}

class LogFormatter extends Formatter
{
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @Override
    public String format(LogRecord record)
    {
        StringBuilder buffer = new StringBuilder();
        return buffer.append(record.getMessage())
                .append(LINE_SEPARATOR).toString();
    }
}
