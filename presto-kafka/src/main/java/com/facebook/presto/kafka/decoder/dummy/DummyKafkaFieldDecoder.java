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
package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Default 'decoder' for the dummy format. Can not decode anything. This is intentional.
 */
public class DummyKafkaFieldDecoder
        implements KafkaFieldDecoder<Void>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return DummyKafkaRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KafkaFieldValueProvider decode(Void value, KafkaColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");

        return new KafkaFieldValueProvider()
        {
            @Override
            public boolean accept(KafkaColumnHandle handle)
            {
                return false;
            }

            @Override
            public boolean isNull()
            {
                throw new PrestoException(KAFKA_CONVERSION_NOT_SUPPORTED, "is null check not supported");
            }
        };
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
