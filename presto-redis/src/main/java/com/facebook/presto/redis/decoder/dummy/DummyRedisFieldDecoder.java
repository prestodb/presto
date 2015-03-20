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
package com.facebook.presto.redis.decoder.dummy;

import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.redis.RedisFieldValueProvider;
import com.facebook.presto.redis.decoder.RedisFieldDecoder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.facebook.presto.redis.RedisErrorCode.REDIS_CONVERSION_NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Default 'decoder' for the dummy format. Can not decode anything. This is intentional.
 */
public class DummyRedisFieldDecoder
        implements RedisFieldDecoder<Void>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return DummyRedisRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return RedisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public RedisFieldValueProvider decode(Void value, RedisColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        return new RedisFieldValueProvider()
        {
            @Override
            public boolean accept(RedisColumnHandle handle)
            {
                return false;
            }

            @Override
            public boolean isNull()
            {
                throw new PrestoException(REDIS_CONVERSION_NOT_SUPPORTED, "is null check not supported");
            }
        };
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
