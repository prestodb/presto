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

import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ConnectorSmallFragmentCoalescingPlan
{
    private final Collection<Slice> retainedFragments;
    private final Collection<Slice> deprecatedFragments;
    private final Stream<SubPlan> subPlans;

    public ConnectorSmallFragmentCoalescingPlan(Collection<Slice> retainedFragments, Collection<Slice> deprecatedFragments, Stream<SubPlan> subPlans)
    {
        this.retainedFragments = requireNonNull(retainedFragments, "retainedFragments is null");
        this.deprecatedFragments = requireNonNull(deprecatedFragments, "deprecatedFragments is null");
        this.subPlans = requireNonNull(subPlans, "tableLayoutHandle is null");
    }

    public Collection<Slice> getRetainedFragments()
    {
        return retainedFragments;
    }

    public Collection<Slice> getDeprecatedFragments()
    {
        return deprecatedFragments;
    }

    public Stream<SubPlan> getSubPlans()
    {
        return subPlans;
    }

    public static ConnectorSmallFragmentCoalescingPlan noOpPlan(Collection<Slice> fragments)
    {
        return new ConnectorSmallFragmentCoalescingPlan(unmodifiableList(new ArrayList<>(fragments)), emptyList(), Stream.empty());
    }

    public static class SubPlan
    {
        private final ConnectorTableLayoutHandle tableLayoutHandle;
        private final List<ColumnHandle> columns;
        private final ConnectorTableHandle connectorTableHandle;

        public SubPlan(ConnectorTableLayoutHandle tableLayoutHandle, List<ColumnHandle> columns, ConnectorTableHandle connectorTableHandle)
        {
            this.tableLayoutHandle = requireNonNull(tableLayoutHandle, "tableLayoutHandle is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.connectorTableHandle = requireNonNull(connectorTableHandle, "connectorTableHandle is null");
        }

        public ConnectorTableLayoutHandle getTableLayoutHandle()
        {
            return tableLayoutHandle;
        }

        public ConnectorTableHandle getConnectorTableHandle()
        {
            return connectorTableHandle;
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }
    }
}
