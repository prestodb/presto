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
package com.facebook.presto.metadata;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableLayoutResult
{
    private final TableLayout layout;
    private final TupleDomain<ColumnHandle> unenforcedConstraint;

    public TableLayoutResult(TableLayout layout, TupleDomain<ColumnHandle> unenforcedConstraint)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
    }

    public TableLayout getLayout()
    {
        return layout;
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }

    public static TupleDomain<ColumnHandle> computeEnforced(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced)
    {
        if (predicate.isNone()) {
            // If the engine requests that the connector provides a layout with a domain of "none". The connector can have two possible reactions, either:
            // 1. The connector can provide an empty table layout.
            //   * There would be no unenforced predicate, i.e., unenforced predicate is TupleDomain.all().
            //   * The predicate was successfully enforced. Enforced predicate would be same as predicate: TupleDomain.none().
            // 2. The connector can't/won't.
            //   * The connector would tell the engine to put a filter on top of the scan, i.e., unenforced predicate is TupleDomain.none().
            //   * The connector didn't successfully enforce anything. Therefore, enforced predicate would be TupleDomain.all().
            if (unenforced.isNone()) {
                return TupleDomain.all();
            }
            if (unenforced.isAll()) {
                return TupleDomain.none();
            }
            throw new IllegalArgumentException();
        }

        // The engine requested the connector provides a layout with a non-none TupleDomain.
        // A TupleDomain is effectively a list of column-Domain pairs.
        // The connector is expected enforce the respective domain entirely on none, some, or all of the columns.
        // 1. When the connector could enforce none of the domains, the unenforced would be equal to predicate;
        // 2. When the connector could enforce some of the domains, the unenforced would contain a subset of the column-Domain pairs;
        // 3. When the connector could enforce all of the domains, the unenforced would be TupleDomain.all().

        // In all 3 cases shown above, the unenforced is not TupleDomain.none().
        checkArgument(!unenforced.isNone());

        Map<ColumnHandle, Domain> predicateDomains = predicate.getDomains().get();
        Map<ColumnHandle, Domain> unenforcedDomains = unenforced.getDomains().get();
        ImmutableMap.Builder<ColumnHandle, Domain> enforcedDomainsBuilder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : predicateDomains.entrySet()) {
            ColumnHandle predicateColumnHandle = entry.getKey();
            if (unenforcedDomains.containsKey(predicateColumnHandle)) {
                checkArgument(
                        entry.getValue().equals(unenforcedDomains.get(predicateColumnHandle)),
                        "Enforced tuple domain cannot be determined. The connector is expected to enforce the respective domain entirely on none, some, or all of the column.");
            }
            else {
                enforcedDomainsBuilder.put(predicateColumnHandle, entry.getValue());
            }
        }
        Map<ColumnHandle, Domain> enforcedDomains = enforcedDomainsBuilder.build();
        checkArgument(
                enforcedDomains.size() + unenforcedDomains.size() == predicateDomains.size(),
                "Enforced tuple domain cannot be determined. Connector returned an unenforced TupleDomain that contains columns not in predicate.");
        return TupleDomain.withColumnDomains(enforcedDomains);
    }
}
