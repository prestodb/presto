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
package com.facebook.presto.pinot;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableMap;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotColumnMetadata
        extends ColumnMetadata
{
    // We need to preserve the case sensitivity of the column, store it here as the super class stores the value after lower-casing it
    private final String name;

    public PinotColumnMetadata(String name, Type type, boolean inNullable, String comment)
    {
        super(requireNonNull(name, "name is null"), requireNonNull(type, "type is null"), inNullable, comment, null, false, ImmutableMap.of());
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name.toLowerCase(ENGLISH);
    }

    public String getPinotName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, getType(), getComment(), isHidden());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PinotColumnMetadata other = (PinotColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getComment(), other.getComment()) &&
                Objects.equals(this.isHidden(), other.isHidden());
    }
}
