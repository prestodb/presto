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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.type.Types;
import com.google.common.base.Function;

import javax.annotation.Nullable;

import static com.facebook.presto.type.Types.fromColumnType;
import static com.google.common.base.Preconditions.checkState;

public enum Type
{
    BIGINT(Types.BIGINT),
    DOUBLE(Types.DOUBLE),
    VARCHAR(Types.VARCHAR),
    BOOLEAN(Types.BOOLEAN),
    NULL(null);

    private final com.facebook.presto.type.Type rawType;

    Type(@Nullable com.facebook.presto.type.Type rawType)
    {
        this.rawType = rawType;
    }

    @Nullable
    public com.facebook.presto.type.Type getRawType()
    {
        return rawType;
    }

    public ColumnType getColumnType()
    {
        checkState(rawType != null, "no column type for null");
        return rawType.toColumnType();
    }

    public String getName()
    {
        return (rawType == null) ? "null" : rawType.getName();
    }

    @Override
    public String toString()
    {
        return getName();
    }

    public static Type fromRaw(ColumnType type)
    {
        return fromRaw(fromColumnType(type));
    }

    public static Type fromRaw(com.facebook.presto.type.Type raw)
    {
        for (Type type : values()) {
            if (type.getRawType() == raw) {
                return type;
            }
        }

        throw new IllegalArgumentException("Can't map raw type to Type: " + raw);
    }

    public static Function<Type, com.facebook.presto.type.Type> toRaw()
    {
        return new Function<Type, com.facebook.presto.type.Type>()
        {
            @Override
            public com.facebook.presto.type.Type apply(Type input)
            {
                return input.getRawType();
            }
        };
    }

    public static boolean isNumeric(Type type)
    {
        return type == BIGINT || type == DOUBLE;
    }

    public static Function<Type, String> nameGetter()
    {
        return new Function<Type, String>()
        {
            @Override
            public String apply(Type type)
            {
                return type.getName();
            }
        };
    }
}
