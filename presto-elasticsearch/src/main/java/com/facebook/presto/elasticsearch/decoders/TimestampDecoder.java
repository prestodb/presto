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
package com.facebook.presto.elasticsearch.decoders;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_TYPE_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;

public class TimestampDecoder
        implements Decoder
{
    private static final ZoneId ZULU = ZoneId.of("Z");

    private final String path;
    private final ZoneId zoneId;

    public TimestampDecoder(ConnectorSession session, String path)
    {
        this.path = requireNonNull(path, "path is null");
        this.zoneId = ZoneId.of(session.getSqlFunctionProperties().getTimeZoneKey().getId());
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        DocumentField documentField = hit.getFields().get(path);
        Object value = null;

        if (documentField != null) {
            if (documentField.getValues().size() > 1) {
                throw new PrestoException(ELASTICSEARCH_TYPE_MISMATCH, format("Expected single value for column '%s', found: %s", path, documentField.getValues().size()));
            }
            value = documentField.getValue();
        }
        else {
            value = getter.get();
        }

        if (value == null) {
            output.appendNull();
        }
        else {
            LocalDateTime timestamp;
            if (value instanceof String) {
                timestamp = ISO_DATE_TIME.parse((String) value, LocalDateTime::from);
            }
            else if (value instanceof Number) {
                timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), ZULU);
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                        path,
                        value.getClass().getSimpleName(),
                        value));
            }

            long epochMillis = timestamp.atZone(zoneId)
                    .toInstant()
                    .toEpochMilli();

            TIMESTAMP.writeLong(output, epochMillis);
        }
    }
}
