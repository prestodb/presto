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
package com.facebook.presto.redis.decoder.hash;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;

class HashRedisValueProvider
        extends FieldValueProvider
{
    protected final DecoderColumnHandle columnHandle;
    protected final String value;

    public HashRedisValueProvider(DecoderColumnHandle columnHandle, String value)
    {
        this.columnHandle = columnHandle;
        this.value = value;
    }

    @Override
    public boolean accept(DecoderColumnHandle handle)
    {
        return columnHandle.equals(handle);
    }

    @Override
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
}
