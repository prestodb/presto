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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Defines a set of valid tuples according to the constraints on each of its constituent columns
 */
public final class TupleDomain<T>
{
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
    private final Map<T, Domain> domains;

    private TupleDomain(Map<T, Domain> domains)
    {
        if (domains == null || containsNoneDomain(domains)) {
            this.domains = null;
        }
        else {
            this.domains = Collections.unmodifiableMap(normalizeAndCopy(domains));
        }
    }

    public static <T> TupleDomain<T> withColumnDomains(Map<T, Domain> domains)
    {
        return new TupleDomain<>(Objects.requireNonNull(domains, "domains is null"));
    }

    public static <T> TupleDomain<T> none()
    {
        return new TupleDomain<>(null);
    }

    public static <T> TupleDomain<T> all()
    {
        return new TupleDomain<>(Collections.<T, Domain>emptyMap());
    }

    /**
     * Convert a map of columns to values into the TupleDomain which requires
     * those columns to be fixed to those values.
     */
    public static <T> TupleDomain<T> withFixedValues(Map<T, Comparable<?>> fixedValues)
    {
        Map<T, Domain> domains = new HashMap<>();
        for (Map.Entry<T, Comparable<?>> entry : fixedValues.entrySet()) {
            domains.put(entry.getKey(), Domain.singleValue(entry.getValue()));
        }
        return withColumnDomains(domains);
    }

    /**
     * Convert a map of columns to values into the TupleDomain which requires
     * those columns to be fixed to those values. Null is allowed as a fixed value.
     */
    public static <T> TupleDomain<T> withNullableFixedValues(Map<T, SerializableNativeValue> fixedValues)
    {
        Map<T, Domain> domains = new HashMap<>();
        for (Map.Entry<T, SerializableNativeValue> entry : fixedValues.entrySet()) {
            if (entry.getValue().getValue() != null) {
                domains.put(entry.getKey(), Domain.singleValue(entry.getValue().getValue()));
            }
            else {
                domains.put(entry.getKey(), Domain.onlyNull(entry.getValue().getType()));
            }
        }
        return withColumnDomains(domains);
    }

    @JsonCreator
    // Available for Jackson deserialization only!
    public static <T> TupleDomain<T> fromNullableColumnDomains(@JsonProperty("nullableColumnDomains") List<ColumnDomain<T>> nullableColumnDomains)
    {
        if (nullableColumnDomains == null) {
            return none();
        }
        return withColumnDomains(toMap(nullableColumnDomains));
    }

    @JsonProperty
    // Available for Jackson serialization only!
    public List<ColumnDomain<T>> getNullableColumnDomains()
    {
        return domains == null ? null : toList(domains);
    }

    private static <T> Map<T, Domain> toMap(List<ColumnDomain<T>> columnDomains)
    {
        Map<T, Domain> map = new HashMap<>();
        for (ColumnDomain<T> columnDomain : columnDomains) {
            if (map.containsKey(columnDomain.getColumnHandle())) {
                throw new IllegalArgumentException("Duplicate column handle!");
            }
            map.put(columnDomain.getColumnHandle(), columnDomain.getDomain());
        }
        return map;
    }

    private static <T> List<ColumnDomain<T>> toList(Map<T, Domain> columnDomains)
    {
        List<ColumnDomain<T>> list = new ArrayList<>();
        for (Map.Entry<T, Domain> entry : columnDomains.entrySet()) {
            list.add(new ColumnDomain<>(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private static <T> boolean containsNoneDomain(Map<T, Domain> domains)
    {
        for (Domain domain : domains.values()) {
            if (domain.isNone()) {
                return true;
            }
        }
        return false;
    }

    private static <T> Map<T, Domain> normalizeAndCopy(Map<T, Domain> domains)
    {
        Map<T, Domain> map = new HashMap<>();
        for (Map.Entry<T, Domain> entry : domains.entrySet()) {
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
    public Map<T, Domain> getDomains()
    {
        if (domains == null) {
            throw new IllegalStateException("Can not get column Domains from a none TupleDomain");
        }
        return domains;
    }

    /**
     * Extract all column constraints that require exactly one value in their respective Domains.
     */
    public Map<T, Comparable<?>> extractFixedValues()
    {
        if (isNone()) {
            return Collections.emptyMap();
        }

        Map<T, Comparable<?>> fixedValues = new HashMap<>();
        for (Map.Entry<T, Domain> entry : getDomains().entrySet()) {
            if (entry.getValue().isSingleValue()) {
                fixedValues.put(entry.getKey(), entry.getValue().getSingleValue());
            }
        }
        return fixedValues;
    }

    /**
    * Extract all column constraints that require exactly one value or only null in their respective Domains.
    */
    public Map<T, SerializableNativeValue> extractNullableFixedValues()
    {
        if (isNone()) {
            return Collections.emptyMap();
        }

        Map<T, SerializableNativeValue> builder = new HashMap<>();
        for (Map.Entry<T, Domain> entry : getDomains().entrySet()) {
            if (entry.getValue().isSingleValue()) {
                builder.put(entry.getKey(), new SerializableNativeValue(entry.getValue().getType(), entry.getValue().getSingleValue()));
            }
            else if (entry.getValue().isOnlyNull()) {
                builder.put(entry.getKey(), new SerializableNativeValue(entry.getValue().getType(), null));
            }
        }
        return builder;
    }

    /**
     * Returns the strict intersection of the TupleDomains.
     * The resulting TupleDomain represents the set of tuples that would would be valid
     * in both TupleDomains.
     */
    public TupleDomain<T> intersect(TupleDomain<T> other)
    {
        if (this.isNone() || other.isNone()) {
            return none();
        }

        Map<T, Domain> intersected = new HashMap<>(this.getDomains());
        for (Map.Entry<T, Domain> entry : other.getDomains().entrySet()) {
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

    @SafeVarargs
    public static <T> TupleDomain<T> columnWiseUnion(TupleDomain<T> first, TupleDomain<T> second, TupleDomain<T>... rest)
    {
        List<TupleDomain<T>> domains = new ArrayList<>();
        domains.add(first);
        domains.add(second);
        domains.addAll(Arrays.asList(rest));

        return columnWiseUnion(domains);
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
    public static <T> TupleDomain<T> columnWiseUnion(List<TupleDomain<T>> tupleDomains)
    {
        if (tupleDomains.isEmpty()) {
            throw new IllegalArgumentException("tupleDomains must have at least one element");
        }

        if (tupleDomains.size() == 1) {
            return tupleDomains.get(0);
        }

        // gather all common columns
        Set<T> commonColumns = new HashSet<>();

        // first, find a non-none domain
        boolean found = false;
        Iterator<TupleDomain<T>> domains = tupleDomains.iterator();
        while (domains.hasNext()) {
            TupleDomain<T> domain = domains.next();
            if (!domain.isNone()) {
                found = true;
                commonColumns.addAll(domain.getDomains().keySet());
                break;
            }
        }

        if (!found) {
            return TupleDomain.none();
        }

        // then, get the common columns
        while (domains.hasNext()) {
            TupleDomain<T> domain = domains.next();
            if (!domain.isNone()) {
                commonColumns.retainAll(domain.getDomains().keySet());
            }
        }

        // group domains by column (only for common columns)
        Map<T, List<Domain>> domainsByColumn = new HashMap<>();

        for (TupleDomain<T> domain : tupleDomains) {
            if (!domain.isNone()) {
                for (Map.Entry<T, Domain> entry : domain.getDomains().entrySet()) {
                    if (commonColumns.contains(entry.getKey())) {
                        List<Domain> domainForColumn = domainsByColumn.get(entry.getKey());
                        if (domainForColumn == null) {
                            domainForColumn = new ArrayList<>();
                            domainsByColumn.put(entry.getKey(), domainForColumn);
                        }
                        domainForColumn.add(entry.getValue());
                    }
                }
            }
        }

        // finally, do the column-wise union
        Map<T, Domain> result = new HashMap<>();
        for (Map.Entry<T, List<Domain>> entry : domainsByColumn.entrySet()) {
            result.put(entry.getKey(), Domain.union(entry.getValue()));
        }
        return withColumnDomains(result);
    }

    /**
     * Returns true only if there exists a strict intersection between the TupleDomains.
     * i.e. there exists some potential tuple that would be allowable in both TupleDomains.
     */
    public boolean overlaps(TupleDomain<T> other)
    {
        return !this.intersect(other).isNone();
    }

    /**
     * Returns true only if the this TupleDomain contains all possible tuples that would be allowable by
     * the other TupleDomain.
     */
    public boolean contains(TupleDomain<T> other)
    {
        return other.isNone() || columnWiseUnion(this, other).equals(this);
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

        TupleDomain<T> that = (TupleDomain<T>) o;

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

    public <U> TupleDomain<U> transform(Function<T, U> function)
    {
        if (domains == null) {
            return new TupleDomain<>(null);
        }

        HashMap<U, Domain> result = new HashMap<>(domains.size());
        for (Map.Entry<T, Domain> entry : domains.entrySet()) {
            U key = function.apply(entry.getKey());

            Domain previous = result.put(key, entry.getValue());

            if (previous != null) {
                throw new IllegalArgumentException(String.format("Every argument must have a unique mapping. %s maps to %s and %s", entry.getKey(), entry.getValue(), previous));
            }
        }

        return new TupleDomain<>(result);
    }

    // Available for Jackson serialization only!
    public static class ColumnDomain<C>
    {
        private final C columnHandle;
        private final Domain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("columnHandle") C columnHandle,
                @JsonProperty("domain") Domain domain)
        {
            this.columnHandle = Objects.requireNonNull(columnHandle, "columnHandle is null");
            this.domain = Objects.requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public C getColumnHandle()
        {
            return columnHandle;
        }

        @JsonProperty
        public Domain getDomain()
        {
            return domain;
        }
    }

    // Custom Function interface because SPI does not include Guava
    public static interface Function<F, T>
    {
        T apply(F input);
    }
}
