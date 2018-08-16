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
package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final Type type;
    private final String comment;
    private final String name;
    private final boolean keyword;
    private final boolean hidden;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("comment") String comment,
            @JsonProperty("keyword") boolean keyword, //5.x
            @JsonProperty("hidden") boolean hidden)
    {
        this.name = requireNonNull(name, "columnName is null");
        this.type = requireNonNull(type, "type is null");

        this.comment = comment;
        this.keyword = keyword;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public boolean getKeyword()
    {
        return keyword;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, comment, hidden);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return Objects.equals(this.name, other.name)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.keyword, other.keyword)
                && Objects.equals(this.hidden, other.hidden)
                && Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("keyword", keyword)
                .add("comment", comment)
                .add("hidden", hidden)
                .toString();
    }
}
