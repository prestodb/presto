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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.spi.predicate.Domain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloColumnConstraint
{
    private final String name;
    private final String family;
    private final String qualifier;
    private final boolean indexed;
    private Optional<Domain> domain;

    @JsonCreator
    public AccumuloColumnConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("family") String family,
            @JsonProperty("qualifier") String qualifier,
            @JsonProperty("domain") Optional<Domain> domain,
            @JsonProperty("indexed") boolean indexed)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.indexed = requireNonNull(indexed, "indexed is null");
        this.domain = requireNonNull(domain, "domain is null");
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getFamily()
    {
        return family;
    }

    @JsonProperty
    public String getQualifier()
    {
        return qualifier;
    }

    @JsonProperty
    public Optional<Domain> getDomain()
    {
        return domain;
    }

    @JsonSetter
    public void setDomain(Optional<Domain> domain)
    {
        this.domain = domain;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, family, qualifier, domain, indexed);
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

        AccumuloColumnConstraint other = (AccumuloColumnConstraint) obj;
        return Objects.equals(this.name, other.name)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.domain, other.domain)
                && Objects.equals(this.indexed, other.indexed);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", this.name)
                .add("family", this.family)
                .add("qualifier", this.qualifier)
                .add("indexed", this.indexed)
                .add("domain", this.domain)
                .toString();
    }
}
