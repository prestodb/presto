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

import com.facebook.presto.spi.type.Type;

import java.util.Objects;

public class ColumnMetadata
{
    private final String name;
    private final Type type;
    private final int ordinalPosition;
    private final boolean partitionKey;
    private final boolean hidden;

    public ColumnMetadata(String name, Type type, int ordinalPosition, boolean partitionKey)
    {
        this(name, type, ordinalPosition, partitionKey, false);
    }

    public ColumnMetadata(String name, Type type, int ordinalPosition, boolean partitionKey, boolean hidden)
    {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }
        if (ordinalPosition < 0) {
            throw new IllegalArgumentException("ordinalPosition is negative");
        }

        this.name = name.toLowerCase();
        this.type = type;
        this.ordinalPosition = ordinalPosition;
        this.partitionKey = partitionKey;
        this.hidden = hidden;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", ordinalPosition=").append(ordinalPosition);
        sb.append(", partitionKey=").append(partitionKey);
        if (hidden) {
            sb.append(", hidden");
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, ordinalPosition, partitionKey, hidden);
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
        ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.partitionKey, other.partitionKey) &&
                Objects.equals(this.hidden, other.hidden);
    }
}
