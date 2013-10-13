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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class Types
{
    private Types()
    {
    }

    public static final Type BOOLEAN = new FixedWidthType("boolean", SIZE_OF_BYTE, ColumnType.BOOLEAN);
    public static final Type BIGINT = new FixedWidthType("bigint", SIZE_OF_LONG, ColumnType.LONG);
    public static final Type DOUBLE = new FixedWidthType("double", SIZE_OF_DOUBLE, ColumnType.DOUBLE);
    public static final Type VARCHAR = new VariableWidthType();

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
            default:
                throw new IllegalStateException("Unknown type " + type);
        }
    }

    @JsonCreator
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
            default:
                throw new IllegalAccessError("Unsupported type " + name);
        }
    }
}
