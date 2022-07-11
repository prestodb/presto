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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JsonPathParameter
        extends Node
{
    private final Identifier name;
    private final Expression parameter;
    private final Optional<JsonFormat> format;

    public JsonPathParameter(Optional<NodeLocation> location, Identifier name, Expression parameter, Optional<JsonFormat> format)
    {
        super(location);

        requireNonNull(name, "name is null");
        requireNonNull(parameter, "parameter is null");
        requireNonNull(format, "format is null");

        this.name = name;
        this.parameter = parameter;
        this.format = format;
    }

    public Identifier getName()
    {
        return name;
    }

    public Expression getParameter()
    {
        return parameter;
    }

    public Optional<JsonFormat> getFormat()
    {
        return format;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(parameter);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JsonPathParameter that = (JsonPathParameter) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(parameter, that.parameter) &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameter, format);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameter", parameter)
                .add("format", format)
                .omitNullValues()
                .toString();
    }

    public enum JsonFormat
    {
        JSON("JSON"),
        UTF8("JSON ENCODING UTF8"),
        UTF16("JSON ENCODING UTF16"),
        UTF32("JSON ENCODING UTF32");

        private final String name;

        JsonFormat(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
