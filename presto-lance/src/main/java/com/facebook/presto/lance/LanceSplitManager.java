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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.lance.Fragment;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private final LanceNamespaceHolder namespaceHolder;

    @Inject
    public LanceSplitManager(LanceNamespaceHolder namespaceHolder)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        LanceTableLayoutHandle layoutHandle = (LanceTableLayoutHandle) layout;
        LanceTableHandle tableHandle = layoutHandle.getTable();

        List<Fragment> fragments = namespaceHolder.getFragments(
                tableHandle.getTableName(),
                tableHandle.getDatasetVersion());

        // With a pushed-down LIMIT, coalesce just enough leading fragments to cover it into a single
        // split instead of one-split-per-fragment (which would scan numFragments * LIMIT rows).
        // getNumRows() is deletion-aware and read from the already-loaded manifest, so this needs no extra IO.
        if (tableHandle.hasLimit()) {
            long limit = tableHandle.getLimit().getAsLong();
            List<Integer> ids = fragments.stream().map(Fragment::getId).collect(toImmutableList());
            List<Long> rowCounts = fragments.stream()
                    .map(fragment -> fragment.metadata().getNumRows())
                    .collect(toImmutableList());
            List<Integer> coalesced = coalesceFragmentsForLimit(ids, rowCounts, limit);
            return new FixedSplitSource(ImmutableList.of(new LanceSplit(coalesced)));
        }

        List<ConnectorSplit> splits = fragments.stream()
                .map(fragment -> (ConnectorSplit) new LanceSplit(
                        ImmutableList.of(fragment.getId())))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }

    /**
     * Returns the leading fragment IDs whose cumulative post-deletion row count covers
     * {@code limit}. {@code rowCounts} must be deletion-aware (e.g.
     * {@code Fragment.metadata().getNumRows()}) so fully-deleted fragments contribute 0
     * and the walk continues past them. Always returns at least one fragment when any exist.
     */
    static List<Integer> coalesceFragmentsForLimit(List<Integer> fragmentIds, List<Long> rowCounts, long limit)
    {
        ImmutableList.Builder<Integer> selected = ImmutableList.builder();
        long accumulated = 0;
        int count = 0;
        for (int i = 0; i < fragmentIds.size() && accumulated < limit; i++) {
            selected.add(fragmentIds.get(i));
            accumulated += rowCounts.get(i);
            count++;
        }
        if (count == 0 && !fragmentIds.isEmpty()) {
            selected.add(fragmentIds.get(0));
        }
        return selected.build();
    }
}
