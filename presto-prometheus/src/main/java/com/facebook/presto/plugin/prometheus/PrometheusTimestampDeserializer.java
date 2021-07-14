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
package com.facebook.presto.plugin.prometheus;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;

import static com.facebook.presto.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static java.time.Instant.ofEpochMilli;

public class PrometheusTimestampDeserializer
        extends JsonDeserializer<Instant>
{
    static Instant decimalEpochTimestampToSQLTimestamp(String timestamp)
    {
        long promTimestampMillis = (long) (Double.parseDouble(timestamp) * 1000);
        return ofEpochMilli(promTimestampMillis);
    }

    @Override
    public Instant deserialize(JsonParser jsonParser, DeserializationContext context)
            throws IOException
    {
        String timestamp = jsonParser.getText().trim();
        try {
            return decimalEpochTimestampToSQLTimestamp(timestamp);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "unable to deserialize timestamp: " + e.getMessage());
        }
    }
}
