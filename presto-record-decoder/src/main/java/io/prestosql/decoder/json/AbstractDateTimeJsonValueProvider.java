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
package io.prestosql.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.util.concurrent.TimeUnit;

import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
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
