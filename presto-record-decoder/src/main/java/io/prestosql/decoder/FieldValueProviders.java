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
package io.prestosql.decoder;

import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * Simple value providers implementations
 */
public final class FieldValueProviders
{
    private FieldValueProviders() {}

    public static FieldValueProvider booleanValueProvider(boolean value)
    {
        return new FieldValueProvider()
        {
            @Override
            public boolean getBoolean()
            {
                return value;
            }

            @Override
            public boolean isNull()
            {
                return false;
            }
        };
    }

    public static FieldValueProvider longValueProvider(long value)
    {
        return new FieldValueProvider()
        {
            @Override
            public long getLong()
            {
                return value;
            }

            @Override
            public boolean isNull()
            {
                return false;
            }
        };
    }

    public static FieldValueProvider bytesValueProvider(byte[] value)
    {
        return new FieldValueProvider()
        {
            @Override
            public Slice getSlice()
            {
                return wrappedBuffer(value);
            }

            @Override
            public boolean isNull()
            {
                return value == null || value.length == 0;
            }
        };
    }

    private static final FieldValueProvider NULL_VALUE_PROVIDER = new FieldValueProvider()
    {
        @Override
        public boolean isNull()
        {
            return true;
        }
    };

    public static FieldValueProvider nullValueProvider()
    {
        return NULL_VALUE_PROVIDER;
    }
}
