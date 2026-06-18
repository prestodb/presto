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
package com.facebook.presto.decoder.json;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static java.lang.String.format;

public abstract class AbstractDateTimeJsonValueProvider
        extends FieldValueProvider
{
    protected final JsonNode value;
    protected final DecoderColumnHandle columnHandle;

    protected AbstractDateTimeJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
    {
        this.value = value;
        this.columnHandle = columnHandle;
    }

    @Override
    public final boolean isNull()
    {
        return value.isMissingNode() || value.isNull();
    }

    @Override
    public final long getLong()
    {
        long millis = getMillis();

        Type type = columnHandle.getType();

        if (type == TIME || type == TIME_WITH_TIME_ZONE) {
            if (millis < 0 || millis >= TimeUnit.DAYS.toMillis(1)) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }

        if (type.equals(DATE)) {
            return TimeUnit.MILLISECONDS.toDays(millis);
        }
        if (type.equals(TIMESTAMP) || type.equals(TIME)) {
            return millis;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME_WITH_TIME_ZONE)) {
            return packDateTimeWithZone(millis, 0);
        }

        return millis;
    }

    /**
     * @return epoch milliseconds in UTC
     */
    protected abstract long getMillis();
}
