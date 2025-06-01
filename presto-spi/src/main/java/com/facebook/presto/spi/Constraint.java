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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class Constraint<T>
{
    private final TupleDomain<T> summary;
    private final Optional<Predicate<Map<T, NullableValue>>> predicate;
    // Optional.empty() when not set
    private final Optional<List<T>> predicateInputs;

    public static <V> Constraint<V> alwaysTrue()
    {
        return new Constraint<>(TupleDomain.<V>all(), Optional.empty());
    }

    public static <V> Constraint<V> alwaysFalse()
    {
        return new Constraint<>(TupleDomain.<V>none(), Optional.of(bindings -> false));
    }

    public Constraint(TupleDomain<T> summary)
    {
        this(summary, Optional.empty());
    }

    public Constraint(TupleDomain<T> summary, Predicate<Map<T, NullableValue>> predicate)
    {
        this(summary, Optional.of(predicate));
    }

    public Constraint(TupleDomain<T> summary, Optional<Predicate<Map<T, NullableValue>>> predicate)
    {
        this(summary, predicate, Optional.empty());
    }

    public Constraint(TupleDomain<T> summary, Optional<Predicate<Map<T, NullableValue>>> predicate, Optional<List<T>> predicateInputs)
    {
        requireNonNull(summary, "summary is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(predicateInputs, "predicateInputs is null");

        this.summary = summary;
        this.predicate = predicate;
        this.predicateInputs = predicateInputs;
    }

    public TupleDomain<T> getSummary()
    {
        return summary;
    }

    public Optional<Predicate<Map<T, NullableValue>>> predicate()
    {
        return predicate;
    }

    public Optional<List<T>> getPredicateInputs()
    {
        return predicateInputs;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Constraint)) {
            return false;
        }

        Constraint other = (Constraint) obj;
        return this.summary.equals(other.summary) && this.predicate.equals(other.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(summary, predicate);
    }
}
