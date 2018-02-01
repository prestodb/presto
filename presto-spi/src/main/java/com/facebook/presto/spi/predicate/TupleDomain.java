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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Defines a set of valid tuples according to the constraints on each of its constituent columns
 */
public final class TupleDomain<T>
{
    /**
     * TupleDomain is internally represented as a normalized map of each column to its
     * respective allowable value Domain. Conceptually, these Domains can be thought of
     * as being AND'ed together to form the representative predicate.
     * <p>
     * This map is normalized in the following ways:
     * 1) The map will not contain Domain.none() as any of its values. If any of the Domain
     * values are Domain.none(), then the whole map will instead be null. This enforces the fact that
     * any single Domain.none() value effectively turns this TupleDomain into "none" as well.
     * 2) The map will not contain Domain.all() as any of its values. Our convention here is that
     * any unmentioned column is equivalent to having Domain.all(). To normalize this structure,
     * we remove any Domain.all() values from the map.
     */
    private final Optional<Map<T, Domain>> domains;

    private TupleDomain(Optional<Map<T, Domain>> domains)
    {
        requireNonNull(domains, "domains is null");

        this.domains = domains.flatMap(map -> {
            if (containsNoneDomain(map)) {
                return Optional.empty();
            }
            return Optional.of(Collections.unmodifiableMap(normalizeAndCopy(map)));
        });
    }

    public static <T> TupleDomain<T> withColumnDomains(Map<T, Domain> domains)
    {
        return new TupleDomain<>(Optional.of(requireNonNull(domains, "domains is null")));
    }

    public static <T> TupleDomain<T> none()
    {
        return new TupleDomain<>(Optional.empty());
    }

    public static <T> TupleDomain<T> all()
    {
        return withColumnDomains(Collections.<T, Domain>emptyMap());
    }

    /**
     * Extract all column constraints that require exactly one value or only null in their respective Domains.
     * Returns an empty Optional if the Domain is none.
     */
    public static <T> Optional<Map<T, NullableValue>> extractFixedValues(TupleDomain<T> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return Optional.empty();
        }

        return Optional.of(tupleDomain.getDomains().get()
                .entrySet().stream()
                .filter(entry -> entry.getValue().isNullableSingleValue())
                .collect(toMap(Map.Entry::getKey, entry -> new NullableValue(entry.getValue().getType(), entry.getValue().getNullableSingleValue()))));
    }

    /**
     * Convert a map of columns to values into the TupleDomain which requires
     * those columns to be fixed to those values. Null is allowed as a fixed value.
     */
    public static <T> TupleDomain<T> fromFixedValues(Map<T, NullableValue> fixedValues)
    {
        return TupleDomain.withColumnDomains(fixedValues.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> {
                            Type type = entry.getValue().getType();
                            Object value = entry.getValue().getValue();
                            return value == null ? Domain.onlyNull(type) : Domain.singleValue(type, value);
                        })));
    }

    @JsonCreator
    // Available for Jackson deserialization only!
    public static <T> TupleDomain<T> fromColumnDomains(@JsonProperty("columnDomains") Optional<List<ColumnDomain<T>>> columnDomains)
    {
        if (!columnDomains.isPresent()) {
            return none();
        }
        return withColumnDomains(columnDomains.get().stream()
                .collect(toMap(ColumnDomain::getColumn, ColumnDomain::getDomain)));
    }

    @JsonProperty
    // Available for Jackson serialization only!
    public Optional<List<ColumnDomain<T>>> getColumnDomains()
    {
        return domains.map(map -> map.entrySet().stream()
                .map(entry -> new ColumnDomain<>(entry.getKey(), entry.getValue()))
                .collect(toList()));
    }

    private static <T> boolean containsNoneDomain(Map<T, Domain> domains)
    {
        return domains.values().stream().anyMatch(Domain::isNone);
    }

    private static <T> Map<T, Domain> normalizeAndCopy(Map<T, Domain> domains)
    {
        return domains.entrySet().stream()
                .filter(entry -> !entry.getValue().isAll())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns true if any tuples would satisfy this TupleDomain
     */
    public boolean isAll()
    {
        return domains.isPresent() && domains.get().isEmpty();
    }

    /**
     * Returns true if no tuple could ever satisfy this TupleDomain
     */
    public boolean isNone()
    {
        return !domains.isPresent();
    }

    /**
     * Gets the TupleDomain as a map of each column to its respective Domain.
     * - Will return an Optional.empty() if this is a 'none' TupleDomain.
     * - Unmentioned columns have an implicit value of Domain.all()
     * - The column Domains can be thought of as AND'ed to together to form the whole predicate
     */
    @JsonIgnore
    public Optional<Map<T, Domain>> getDomains()
    {
        return domains;
    }

    /**
     * Returns the strict intersection of the TupleDomains.
     * The resulting TupleDomain represents the set of tuples that would be valid
     * in both TupleDomains.
     */
    public TupleDomain<T> intersect(TupleDomain<T> other)
    {
        if (this.isNone() || other.isNone()) {
            return none();
        }

        Map<T, Domain> intersected = new HashMap<>(this.getDomains().get());
        for (Map.Entry<T, Domain> entry : other.getDomains().get().entrySet()) {
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
        List<TupleDomain<T>> domains = new ArrayList<>(rest.length + 2);
        domains.add(first);
        domains.add(second);
        domains.addAll(Arrays.asList(rest));

        return columnWiseUnion(domains);
    }

    /**
     * Returns a TupleDomain in which corresponding column Domains are unioned together.
     * <p>
     * Note that this is NOT equivalent to a strict union as the final result may allow tuples
     * that do not exist in either TupleDomain.
     * For example:
     * <p>
     * <ul>
     * <li>TupleDomain X: a => 1, b => 2
     * <li>TupleDomain Y: a => 2, b => 3
     * <li>Column-wise unioned TupleDomain: a = > 1 OR 2, b => 2 OR 3
     * </ul>
     * <p>
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
                commonColumns.addAll(domain.getDomains().get().keySet());
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
                commonColumns.retainAll(domain.getDomains().get().keySet());
            }
        }

        // group domains by column (only for common columns)
        Map<T, List<Domain>> domainsByColumn = new HashMap<>(tupleDomains.size());

        for (TupleDomain<T> domain : tupleDomains) {
            if (!domain.isNone()) {
                for (Map.Entry<T, Domain> entry : domain.getDomains().get().entrySet()) {
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
        Map<T, Domain> result = new HashMap<>(domainsByColumn.size());
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TupleDomain other = (TupleDomain) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domains);
    }

    public String toString(ConnectorSession session)
    {
        StringBuilder buffer = new StringBuilder()
                .append("TupleDomain:");
        if (isAll()) {
            buffer.append("ALL");
        }
        else if (isNone()) {
            buffer.append("NONE");
        }
        else {
            buffer.append(domains.get().entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().toString(session))));
        }
        return buffer.toString();
    }

    public <U> TupleDomain<U> transform(Function<T, U> function)
    {
        if (!domains.isPresent()) {
            return TupleDomain.none();
        }

        HashMap<U, Domain> result = new HashMap<>(domains.get().size());
        for (Map.Entry<T, Domain> entry : domains.get().entrySet()) {
            U key = function.apply(entry.getKey());

            if (key == null) {
                continue;
            }

            Domain previous = result.put(key, entry.getValue());

            if (previous != null) {
                throw new IllegalArgumentException(String.format("Every argument must have a unique mapping. %s maps to %s and %s", entry.getKey(), entry.getValue(), previous));
            }
        }

        return TupleDomain.withColumnDomains(result);
    }

    public TupleDomain<T> simplify()
    {
        if (isNone()) {
            return this;
        }

        Map<T, Domain> simplified = domains.get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().simplify()));

        return TupleDomain.withColumnDomains(simplified);
    }

    // Available for Jackson serialization only!
    public static class ColumnDomain<C>
    {
        private final C column;
        private final Domain domain;

        @JsonCreator
        public ColumnDomain(
                @JsonProperty("column") C column,
                @JsonProperty("domain") Domain domain)
        {
            this.column = requireNonNull(column, "column is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public C getColumn()
        {
            return column;
        }

        @JsonProperty
        public Domain getDomain()
        {
            return domain;
        }
    }
}
