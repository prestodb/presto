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
package com.facebook.presto.type;

import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Function;

import javax.annotation.Nullable;

import static com.facebook.presto.type.BigintType.BIGINT;
import static com.facebook.presto.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.NullType.NULL;
import static com.facebook.presto.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Types
{
    private Types()
    {
    }

    public static Type fromColumnType(ColumnType type)
    {
        switch (type) {
            case BOOLEAN:
                return BOOLEAN;
            case DOUBLE:
                return DOUBLE;
            case LONG:
                return BIGINT;
            case STRING:
                return VARCHAR;
            case NULL:
                return NULL;
            default:
                throw new IllegalStateException("Unknown type " + type);
        }
    }

    @JsonCreator
    @Nullable
    public static Type fromName(String name)
    {
        checkNotNull(name, "name is null");
        switch (name) {
            case "bigint":
                return BIGINT;
            case "varchar":
                return VARCHAR;
            case "double":
                return DOUBLE;
            case "boolean":
                return BOOLEAN;
            case "null":
                return NULL;
            default:
                return null;
        }
    }

    public static boolean isNumeric(Type type)
    {
        return type.equals(BIGINT) || type.equals(DOUBLE);
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
