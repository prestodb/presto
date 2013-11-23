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
package com.facebook.presto.spi;

import java.util.Objects;

public enum ColumnType
{
    BOOLEAN(Boolean.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    STRING(String.class);

    private final Class<?> nativeType;

    private ColumnType(Class<?> nativeType)
    {
        this.nativeType = Objects.requireNonNull(nativeType, "nativeType is null");
    }

    public Class<?> getNativeType()
    {
        return nativeType;
    }

    public static ColumnType fromNativeType(Class<?> nativeType)
    {
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType.getNativeType().equals(nativeType)) {
                return columnType;
            }
        }
        throw new IllegalArgumentException(String.format("No native column type found for %s", nativeType));
    }
}
