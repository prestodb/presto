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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class JdbcColumnMetadata
        extends ColumnMetadata
{
    private final String originalName;

    public JdbcColumnMetadata(String name, Type type)
    {
        this(name, type, true, null, null, false, emptyMap());
    }

    /**
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    public JdbcColumnMetadata(String name, Type type, boolean nullable, String comment, String extraInfo, boolean hidden, Map<String, Object> properties)
    {
        super(ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .setComment(comment)
                .setExtraInfo(extraInfo)
                .setHidden(hidden)
                .setProperties(properties));
        this.originalName = requireNonNull(name, "name cannot be null");
    }

    public String getName()
    {
        return originalName;
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
        JdbcColumnMetadata other = (JdbcColumnMetadata) obj;
        return Objects.equals(this.originalName, other.originalName) &&
                Objects.equals(this.getName(), other.getName()) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getComment(), other.getComment()) &&
                Objects.equals(this.isHidden(), other.isHidden()) &&
                Objects.equals(this.isNullable(), other.isNullable()) &&
                Objects.equals(this.getProperties(), other.getProperties());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(originalName, getType(), getComment(), isHidden(), isNullable(), getProperties());
    }

    public static Builder jdbcBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private Type type;
        private boolean nullable = true;
        private Optional<String> comment = Optional.empty();
        private Optional<String> extraInfo = Optional.empty();
        private boolean hidden;
        private Map<String, Object> properties = emptyMap();

        private Builder() {}

        public Builder setName(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder setType(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            return this;
        }

        public Builder setNullable(boolean nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setComment(String comment)
        {
            this.comment = Optional.ofNullable(comment);
            return this;
        }

        public Builder setExtraInfo(String extraInfo)
        {
            this.extraInfo = Optional.ofNullable(extraInfo);
            return this;
        }

        public Builder setHidden(boolean hidden)
        {
            this.hidden = hidden;
            return this;
        }

        public Builder setProperties(Map<String, Object> properties)
        {
            this.properties = requireNonNull(properties, "properties is null");
            return this;
        }

        public JdbcColumnMetadata build()
        {
            return new JdbcColumnMetadata(
                    name,
                    type,
                    nullable,
                    comment.orElse(null),
                    extraInfo.orElse(null),
                    hidden,
                    properties);
        }
    }
}
