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
package com.facebook.presto.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static java.lang.String.format;

public class Identity
{
    private Type type;
    private String name;

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Identity setType(Type type)
    {
        this.type = type;
        return this;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Identity setName(String name)
    {
        this.name = name;
        return this;
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
        Identity o = (Identity) obj;
        return Objects.equal(this.type, o.type) &&
                Objects.equal(this.name, o.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, name);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .toString();
    }

    public enum Type
    {
        USER_NAME("user_name"),
        KERBEROS_PRINCIPAL("kerberos_principal");

        private final String value;

        private Type(String value)
        {
            this.value = value;
        }

        @JsonCreator
        public static Type getType(String value)
        {
            if (USER_NAME.getValue().equalsIgnoreCase(value)) {
                return USER_NAME;
            }
            if (KERBEROS_PRINCIPAL.getValue().equalsIgnoreCase(value)) {
                return KERBEROS_PRINCIPAL;
            }
            throw new IllegalArgumentException(format("delegation identity type %s is not supported", value));
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return value;
        }
    }
}
