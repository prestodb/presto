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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nullable;

import java.time.LocalDate;

/**
 * Custom codec to map Cassandra INT to Java LocalDate.
 * This is needed because we store DATE values as INT (days since
 * epoch) to avoid the "date" reserved keyword issue in Cassandra.
 * <p>
 * The codec converts between:
 * - Cassandra INT (days since epoch, 32-bit integer)
 * - Java LocalDate (which internally uses days since epoch)
 */
public final class IntToLocalDateCodec
        extends MappingCodec<Integer, LocalDate>
{
    public IntToLocalDateCodec()
    {
        super(TypeCodecs.INT, GenericType.of(LocalDate.class));
    }

    @Override
    public GenericType<LocalDate> getJavaType()
    {
        return GenericType.of(LocalDate.class);
    }

    @Nullable
    @Override
    protected LocalDate innerToOuter(@Nullable final Integer value)
    {
        if (value == null) {
            return null;
        }
        return LocalDate.ofEpochDay(value.longValue());
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable final LocalDate value)
    {
        if (value == null) {
            return null;
        }
        return (int) value.toEpochDay();
    }
}
