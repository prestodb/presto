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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

/**
 * Base class for all providers that return values for a selected column.
 */
public abstract class KafkaFieldValueProvider
{
    public abstract boolean accept(KafkaColumnHandle columnHandle);

    public boolean getBoolean()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to boolean not supported");
    }

    public long getLong()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to long not supported");
    }

    public double getDouble()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to double not supported");
    }

    public Slice getSlice()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to Slice not supported");
    }

    public abstract boolean isNull();
}
