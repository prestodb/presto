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
package io.prestosql.connector.system.jdbc;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Optional;

final class FilterUtil
{
    private FilterUtil() {}

    public static Optional<String> stringFilter(TupleDomain<Integer> constraint, int index)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Domain domain = constraint.getDomains().get().get(index);
        if ((domain == null) || !domain.isSingleValue()) {
            return Optional.empty();
        }

        Object value = domain.getSingleValue();
        if (value instanceof Slice) {
            return Optional.of(((Slice) value).toStringUtf8());
        }
        return Optional.empty();
    }

    public static QualifiedTablePrefix tablePrefix(String catalog, Optional<String> schema, Optional<String> table)
    {
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalog);
        if (schema.isPresent()) {
            prefix = new QualifiedTablePrefix(catalog, schema.get());
            if (table.isPresent()) {
                prefix = new QualifiedTablePrefix(catalog, schema.get(), table.get());
            }
        }
        return prefix;
    }

    public static <T> Iterable<T> filter(Iterable<T> items, Optional<T> filter)
    {
        if (!filter.isPresent()) {
            return items;
        }
        return Iterables.filter(items, Predicates.equalTo(filter.get()));
    }

    public static <T> boolean emptyOrEquals(Optional<T> value, T other)
    {
        return !value.isPresent() || value.get().equals(other);
    }
}
