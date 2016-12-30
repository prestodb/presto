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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MongoColumnHandle
        implements ColumnHandle
{
    public static final String SAMPLE_WEIGHT_COLUMN_NAME = "presto_sample_weight";

    private final String name;
    private final String alias;
    private final Type type;
    private final boolean hidden;

    @JsonCreator
    public MongoColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("alias") String alias,
            @JsonProperty("columnType") Type type,
            @JsonProperty("hidden") boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.alias = alias == null ? name.toLowerCase(ENGLISH) : alias;
        this.type = requireNonNull(type, "columnType is null");
        this.hidden = hidden;

        checkArgument(this.alias.toLowerCase(ENGLISH).equals(this.alias), "alias must consist of lowercase letters");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getAlias()
    {
        return alias;
    }

    @JsonProperty("columnType")
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata toColumnMetadata()
    {
        // should be an alias (lower case name)
        return new ColumnMetadata(alias, type, null, hidden);
    }

    public Document getDocument()
    {
        return new Document().append("name", name)
                .append("alias", alias)
                .append("type", type.getTypeSignature().toString())
                .append("hidden", hidden);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, alias, type, hidden);
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
        MongoColumnHandle other = (MongoColumnHandle) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(alias, other.alias) &&
                Objects.equals(type, other.type) &&
                Objects.equals(hidden, other.hidden);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("alias", alias)
                .add("type", type)
                .add("hidden", hidden)
                .toString();
    }
}
