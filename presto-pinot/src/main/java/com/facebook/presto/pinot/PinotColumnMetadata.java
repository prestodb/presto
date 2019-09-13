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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;

public class PinotColumnMetadata
        extends ColumnMetadata
{
    private final String name;
    private final Type type;
    private final String comment;
    private final boolean isHidden;

    public PinotColumnMetadata(String name, Type type)
    {
        this(name, type, null, false);
    }

    public PinotColumnMetadata(String name, Type type, String comment, boolean isHidden)
    {
        super(name, type, comment, isHidden);
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }

        this.name = name;
        this.type = type;
        this.comment = comment;
        this.isHidden = isHidden;
    }

    public String getName()
    {
        return name.toLowerCase(ENGLISH);
    }

    public String getPinotName()
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
        return isHidden;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .add("isHidden", isHidden)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, isHidden);
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
        return Objects.equals(this.name, other.name) && Objects.equals(this.type, other.type) && Objects.equals(this.comment, other.comment) && Objects.equals(this.isHidden, other.isHidden);
    }
}
