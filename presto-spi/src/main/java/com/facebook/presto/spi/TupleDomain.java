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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines a set of valid tuples according to the constraints on each of its constituent columns
 */
public final class TupleDomain
{
    private static final TupleDomain NONE = new TupleDomain(null);
    private static final TupleDomain ALL = new TupleDomain(Collections.<ColumnHandle, Domain>emptyMap());

    /**
     * TupleDomain is internally represented as a normalized map of each column to its
     * respective allowable value Domain. Conceptually, these Domains can be thought of
     * as being AND'ed together to form the representative predicate.
     *
     * This map is normalized in the following ways:
     * 1) The map will not contain Domain.none() as any of its values. If any of the Domain
     * values are Domain.none(), then the whole map will instead be null. This enforces the fact that
     * any single Domain.none() value effectively turns this TupleDomain into "none" as well.
     * 2) The map will not contain Domain.all() as any of its values. Our convention here is that
     * any unmentioned column is equivalent to having Domain.all(). To normalize this structure,
     * we remove any Domain.all() values from the map.
     */
    private final Map<ColumnHandle, Domain> domains;

    private TupleDomain(Map<ColumnHandle, Domain> domains)
    {
        if (domains == null || containsNoneDomain(domains)) {
            this.domains = null;
        }
        else {
            this.domains = Collections.unmodifiableMap(normalizeAndCopy(domains));
        }
    }

    public static TupleDomain withColumnDomains(Map<ColumnHandle, Domain> domains)
    {
        return new TupleDomain(Objects.requireNonNull(domains, "domains is null"));
    }

    public static TupleDomain none()
    {
        return NONE;
    }

    public static TupleDomain all()
    {
        return ALL;
    }

    /**
     * Convert a map of columns to values into the TupleDomain which requires
     * those columns to be fixed to those values.
     */
    public static TupleDomain withFixedValues(Map<ColumnHandle, Comparable<?>> fixedValues)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>();
        for (Map.Entry<ColumnHandle, Comparable<?>> entry : fixedValues.entrySet()) {
            domains.put(entry.getKey(), Domain.singleValue(entry.getValue()));
        }
        return withColumnDomains(domains);
    }

    @JsonCreator
    // Available for Jackson deserialization only!
    public static TupleDomain fromNullableColumnDomains(@JsonProperty("nullableColumnDomains") List<ColumnDomain> nullableColumnDomains)
    {
        if (nullableColumnDomains == null) {
            return none();
        }
        return withColumnDomains(toMap(nullableColumnDomains));
    }

    @JsonProperty
    // Available for Jackson serialization only!
    public List<ColumnDomain> getNullableColumnDomains()
    {
        return domains == null ? null : toList(domains);
    }

    private static Map<ColumnHandle, Domain> toMap(List<ColumnDomain> columnDomains)
    {
        Map<ColumnHandle, Domain> map = new HashMap<>();
        for (ColumnDomain columnDomain : columnDomains) {
            if (map.containsKey(columnDomain.getColumnHandle())) {
                throw new IllegalArgumentException("Duplicate column handle!");
            }
            map.put(columnDomain.getColumnHandle(), columnDomain.getDomain());
        }
        return map;
    }

    private static List<ColumnDomain> toList(Map<ColumnHandle, Domain> columnDomains)
    {
        List<ColumnDomain> list = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : columnDomains.entrySet()) {
            list.add(new ColumnDomain(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private static boolean containsNoneDomain(Map<ColumnHandle, Domain> domains)
    {
        for (Domain domain : domains.values()) {
            if (domain.isNone()) {
                return true;
            }
        }
        return false;
    }

    private static Map<ColumnHandle, Domain> normalizeAndCopy(Map<ColumnHandle, Domain> domains)
    {
        Map<ColumnHandle, Domain> map = new HashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            if (!entry.getValue().isAll()) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    /**
     * Returns true if any tuples would satisfy this TupleDomain
     */
    @JsonIgnore
    public boolean isAll()
    {
        return domains != null && domains.isEmpty();
    }

    /**
     * Returns true if no tuple could ever satisfy this TupleDomain
     */
    @JsonIgnore
    public boolean isNone()
    {
        return domains == null;
    }

    /**
     * Gets the TupleDomain as a map of each column to its respective Domain.
     * - You must check to make sure that this TupleDomain is not None before calling this method
     * - Unmentioned columns have an implicit value of Domain.all()
     * - The column Domains can be thought of as AND'ed to together to form the whole predicate
     */
    @JsonIgnore
    public Map<ColumnHandle, Domain> getDomains()
    {
        if (domains == null) {
            throw new IllegalStateException("Can not get column Domains from a none TupleDomain");
        }
        return domains;
    }

    /**
     * Extract all column constraints that require exactly one value in their respective Domains.
     */
    public Map<ColumnHandle, Comparable<?>> extractFixedValues()
    {
        if (isNone()) {
            return Collections.emptyMap();
        }

        Map<ColumnHandle, Comparable<?>> fixedValues = new HashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : getDomains().entrySet()) {
            if (entry.getValue().isSingleValue()) {
                fixedValues.put(entry.getKey(), entry.getValue().getSingleValue());
            }
        }
        return fixedValues;
    }

    /**
     * Returns the strict intersection of the TupleDomains.
     * The resulting TupleDomain represents the set of tuples that would would be valid
     * in both TupleDomains.
     */
    public TupleDomain intersect(TupleDomain other)
    {
        if (this.isNone() || other.isNone()) {
            return none();
        }

        Map<ColumnHandle, Domain> intersected = new HashMap<>(this.getDomains());
        for (Map.Entry<ColumnHandle, Domain> entry : other.getDomains().entrySet()) {
            Domain intersectionDomain = intersected.get(entry.getKey());
            if (intersectionDomain == null) {
                intersected.put(entry.getKey(), entry.getValue());
            }
            else {
                intersected.put(entry.getKey(), intersectionDomain.intersect(entry.getValue()));
            }
        }
        return withColumnDomains(intersected);
    }

    /**
     * Returns a TupleDomain in which corresponding column Domains are unioned together.
     *
     * Note that this is NOT equivalent to a strict union as the final result may allow tuples
     * that do not exist in either TupleDomain.
     * For example:
     *   TupleDomain X: a => 1, b => 2
     *   TupleDomain Y: a => 2, b => 3
     *   Column-wise unioned TupleDomain: a = > 1 OR 2, b => 2 OR 3
     * In the above resulting TupleDomain, tuple (a => 1, b => 3) would be considered valid but would
     * not be valid for either TupleDomain X or TupleDomain Y.
     * However, this result is guaranteed to be a superset of the strict union.
     */
    public TupleDomain columnWiseUnion(TupleDomain other)
    {
        if (this.isNone()) {
            return other;
        }
        else if (other.isNone()) {
            return this;
        }

        // Only columns contained in both TupleDomains will make it into the column-wise union.
        // This is b/c an unmentioned column is implicitly an "all" Domain and so any union with that "all" Domain will also be the "all" Domain.
        Map<ColumnHandle, Domain> columnWiseUnioned = new HashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : this.getDomains().entrySet()) {
            Domain otherDomain = other.getDomains().get(entry.getKey());
            if (otherDomain != null) {
                columnWiseUnioned.put(entry.getKey(), entry.getValue().union(otherDomain));
            }
        }
        return withColumnDomains(columnWiseUnioned);
    }

    /**
     * Returns true only if there exists a strict intersection between the TupleDomains.
     * i.e. there exists some potential tuple that would be allowable in both TupleDomains.
     */
    public boolean overlaps(TupleDomain other)
    {
        return !this.intersect(other).isNone();
    }

    /**
     * Returns true only if the this TupleDomain contains all possible tuples that would be allowable by
     * the other TupleDomain.
     */
    public boolean contains(TupleDomain other)
    {
        return other.isNone() || this.columnWiseUnion(other).equals(this);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleDomain)) {
            return false;
        }

        TupleDomain that = (TupleDomain) o;

        if (domains != null ? !domains.equals(that.domains) : that.domains != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return domains != null ? domains.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder()
                .append("TupleDomain:");
        if (isAll()) {
            builder.append("ALL");
        }
        else if (isNone()) {
            builder.append("NONE");
        }
        else {
            builder.append(domains);
        }
        return builder.toString();
    }

    // Available for Jackson serialization only!
    public static class ColumnDomain
    {
        private final ColumnHandle columnHandle;
        private final Domain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("columnHandle") ColumnHandle columnHandle,
                @JsonProperty("domain") Domain domain)
        {
            this.columnHandle = Objects.requireNonNull(columnHandle, "columnHandle is null");
            this.domain = Objects.requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        @JsonProperty
        public Domain getDomain()
        {
            return domain;
        }
    }
}
