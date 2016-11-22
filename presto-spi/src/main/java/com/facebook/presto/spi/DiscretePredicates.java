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

import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class DiscretePredicates
{
    private final List<ColumnHandle> columns;
    private final Iterable<TupleDomain<ColumnHandle>> predicates;

    public DiscretePredicates(List<ColumnHandle> columns, Iterable<TupleDomain<ColumnHandle>> predicates)
    {
        requireNonNull(columns, "columns is null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns is empty");
        }
        this.columns = unmodifiableList(new ArrayList<>(columns));
        // do not copy predicates because it may be lazy
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    public List<ColumnHandle> getColumns()
    {
        return columns;
    }

    public Iterable<TupleDomain<ColumnHandle>> getPredicates()
    {
        return predicates;
    }
}
