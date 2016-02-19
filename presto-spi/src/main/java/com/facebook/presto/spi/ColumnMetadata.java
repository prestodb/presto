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

import static java.util.Locale.ENGLISH;

public class ColumnMetadata
{
    private final String name;
    private final Type type;
    private final String comment;
    private final boolean hidden;

    public ColumnMetadata(String name, Type type)
    {
        this(name, type, null, false);
    }

    public ColumnMetadata(String name, Type type, String comment, boolean hidden)
    {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.comment = comment;
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

    public String getComment()
    {
        return comment;
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
        if (comment != null) {
            sb.append(", comment='").append(comment).append('\'');
        }
        if (hidden) {
            sb.append(", hidden");
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, hidden);
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
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.hidden, other.hidden);
    }
}
