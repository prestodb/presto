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
package com.facebook.presto.kinesis.decoder.csv;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import io.airlift.slice.Slice;

import java.util.Set;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.google.common.collect.ImmutableSet;

public class CsvKinesisFieldDecoder
        implements KinesisFieldDecoder<String>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public String getRowDecoderName()
    {
        return CsvKinesisRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KinesisFieldValueProvider decode(final String value, final KinesisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        return new KinesisFieldValueProvider()
        {
            @Override
            public boolean accept(KinesisColumnHandle handle)
            {
                return columnHandle.equals(handle);
            }

            public boolean isNull()
            {
                return (value == null) || value.isEmpty();
            }

            @SuppressWarnings("SimplifiableConditionalExpression")
            @Override
            public boolean getBoolean()
            {
                return isNull() ? false : Boolean.parseBoolean(value.trim());
            }

            @Override
            public long getLong()
            {
                return isNull() ? 0L : Long.parseLong(value.trim());
            }

            @Override
            public double getDouble()
            {
                return isNull() ? 0.0d : Double.parseDouble(value.trim());
            }

            @Override
            public Slice getSlice()
            {
                return isNull() ? EMPTY_SLICE : utf8Slice(value);
            }
        };
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
