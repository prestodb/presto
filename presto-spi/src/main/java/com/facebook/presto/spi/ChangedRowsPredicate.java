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

import com.facebook.presto.common.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Connector-provided predicates that identify rows changed since an MV was last materialized.
 */
public class ChangedRowsPredicate
{
    private final List<TupleDomain<ColumnHandle>> dataDisjuncts;
    private final TupleDomain<ColumnHandle> refreshBound;

    public ChangedRowsPredicate(List<TupleDomain<ColumnHandle>> dataDisjuncts, TupleDomain<ColumnHandle> refreshBound)
    {
        this.dataDisjuncts = unmodifiableList(new ArrayList<>(requireNonNull(dataDisjuncts, "dataDisjuncts is null")));
        this.refreshBound = requireNonNull(refreshBound, "refreshBound is null");
    }

    public static ChangedRowsPredicate empty()
    {
        return new ChangedRowsPredicate(emptyList(), TupleDomain.all());
    }

    public List<TupleDomain<ColumnHandle>> getDataDisjuncts()
    {
        return dataDisjuncts;
    }

    public TupleDomain<ColumnHandle> getRefreshBound()
    {
        return refreshBound;
    }
}
