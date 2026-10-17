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
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Connector-provided predicates that identify rows changed since an MV was last materialized.
 * <p>
 * The disjuncts describe the rows of the table <em>as it now stands</em> that the range touched.
 * That shape cannot express two things a consumer needs, so each is carried separately:
 * <ul>
 * <li>whether the range only added rows -- a modified row is still present and still matches the
 * disjuncts, so matching alone cannot tell an addition from a modification; and</li>
 * <li>where the rows the range removed can be read -- a removed row is not in the table to be
 * selected, so no predicate over the table can find one.</li>
 * </ul>
 */
public class ChangedRowsPredicate
{
    private final List<TupleDomain<ColumnHandle>> dataDisjuncts;
    private final TupleDomain<ColumnHandle> refreshBound;
    private final boolean additionsOnly;
    private final Optional<RemovedRows> removedRows;

    /**
     * @param additionsOnly whether every row the disjuncts match was added within this version
     *         range, so that no row present at the recorded version was modified or removed. A
     *         consumer that may leave a materialized row in place -- rather than recomputing
     *         whatever the change touched -- is only correct when this holds.
     * @param removedRows where to read the rows the range took away, when the connector can
     *         produce them. Absent means it cannot, which is not the same as there being none.
     */
    public ChangedRowsPredicate(
            List<TupleDomain<ColumnHandle>> dataDisjuncts,
            TupleDomain<ColumnHandle> refreshBound,
            boolean additionsOnly,
            Optional<RemovedRows> removedRows)
    {
        this.dataDisjuncts = unmodifiableList(new ArrayList<>(requireNonNull(dataDisjuncts, "dataDisjuncts is null")));
        this.refreshBound = requireNonNull(refreshBound, "refreshBound is null");
        this.additionsOnly = additionsOnly;
        this.removedRows = requireNonNull(removedRows, "removedRows is null");
    }

    /**
     * Reads as a connector that cannot report what the range removed.
     */
    public ChangedRowsPredicate(
            List<TupleDomain<ColumnHandle>> dataDisjuncts,
            TupleDomain<ColumnHandle> refreshBound,
            boolean additionsOnly)
    {
        this(dataDisjuncts, refreshBound, additionsOnly, Optional.empty());
    }

    /**
     * Reads as not knowing whether the range holds modifications -- the conservative answer, since
     * claiming additions only when a row was in fact modified would leave the stale materialized
     * row in place.
     */
    public ChangedRowsPredicate(List<TupleDomain<ColumnHandle>> dataDisjuncts, TupleDomain<ColumnHandle> refreshBound)
    {
        this(dataDisjuncts, refreshBound, false);
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

    public boolean isAdditionsOnly()
    {
        return additionsOnly;
    }

    public Optional<RemovedRows> getRemovedRows()
    {
        return removedRows;
    }

    /**
     * A relation holding the rows a version range removed.
     * <p>
     * The connector names this relation rather than the engine constructing it, for two reasons.
     * How a version range is addressed is the connector's own business, so expressing it in the
     * engine would put one connector's naming there. And the connector already holds both versions
     * of the range at the moment it decides the range is reportable, so the decision and the
     * relation come from one pinned pair; were the engine to derive the upper bound itself, a
     * commit landing in between would leave it reading a different range than the predicate was
     * built for, silently missing whatever the gap removed.
     * <p>
     * The contract: scanning {@link #getTable()} with the column handles the connector reports for
     * it yields at least one row for every row the range removed, and possibly more.
     * Over-reporting is allowed, because a consumer computing which rows a change touched may
     * safely recompute more than it had to. Under-reporting is not, which is why a connector that
     * cannot cover the whole range leaves this absent rather than reporting part of it. The removed
     * row's own values are in the single column named by {@link #getRowColumn()}, whose type is a
     * row whose fields carry the names of the table's columns.
     */
    public static final class RemovedRows
    {
        private final ConnectorTableHandle table;
        private final String rowColumn;

        public RemovedRows(ConnectorTableHandle table, String rowColumn)
        {
            this.table = requireNonNull(table, "table is null");
            this.rowColumn = requireNonNull(rowColumn, "rowColumn is null");
        }

        public ConnectorTableHandle getTable()
        {
            return table;
        }

        public String getRowColumn()
        {
            return rowColumn;
        }
    }
}
