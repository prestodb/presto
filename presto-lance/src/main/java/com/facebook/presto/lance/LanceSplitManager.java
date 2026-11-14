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
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

        String datasetPath = tableHandle.getTablePath();

        List<Fragment> fragments = namespaceHolder.getFragments(
                tableHandle.getTablePath(),
                tableHandle.getDatasetVersion());

        // Vector (ANN) search. Default: a single split over all fragments — one nearest query,
        // exact global top-k. With vector_search_per_segment_splits (native execution only): one
        // split per vector-index segment, using the segment's fragment coverage; each Velox worker
        // runs a fragment-scoped nearest (fragment IDs become a prefilter row mask, which is the
        // only form of fragment-scoped nearest Lance permits) and the engine-side global TopN
        // merges the per-segment top-k lists into the exact global top-k. Fragments partition rows
        // across splits, so no row can appear in two splits. The Java page source cannot take this
        // path: lance-core silently downgrades a nearest scan with fragment IDs to a plain scan,
        // which is why the property defaults to off.
        if (tableHandle.hasVectorSearch()) {
            List<Integer> allFragmentIds = fragments.stream().map(Fragment::getId).collect(toImmutableList());
            if (session != null && LanceSessionProperties.isVectorSearchPerSegmentSplits(session)) {
                Dataset dataset = namespaceHolder.getCachedDataset(datasetPath, tableHandle.getDatasetVersion());
                List<List<Integer>> segmentLists = segmentFragmentLists(findVectorIndexSegments(
                        dataset,
                        tableHandle.getVectorSearch().get().getColumn()));
                return new FixedSplitSource(ImmutableList.copyOf(
                        buildVectorSearchSplits(datasetPath, allFragmentIds, segmentLists)));
            }
            return new FixedSplitSource(ImmutableList.of(new LanceSplit(datasetPath, allFragmentIds)));
        }

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
            return new FixedSplitSource(ImmutableList.of(new LanceSplit(datasetPath, coalesced)));
        }

        List<ConnectorSplit> splits = fragments.stream()
                .map(fragment -> (ConnectorSplit) new LanceSplit(
                        datasetPath,
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

    /**
     * Builds the splits for a vector search: one split per index segment (using the segment's
     * fragment coverage) plus one split for any fragments not covered by a segment. When no
     * segments exist (no vector index, or an empty index), returns a single split over all
     * fragments so Lance flat-searches the whole dataset. Always returns at least one split when
     * fragments exist; segments contributing no fragments are skipped.
     *
     * <p>Used by {@code getSplits} when {@code vector_search_per_segment_splits} is enabled:
     * fragment-scoped nearest runs on native (Velox) workers via prefilter, and this method
     * partitions fragments across splits by index segment.
     */
    static List<LanceSplit> buildVectorSearchSplits(String datasetPath, List<Integer> allFragmentIds, List<List<Integer>> segmentFragmentLists)
    {
        if (segmentFragmentLists.isEmpty()) {
            return ImmutableList.of(new LanceSplit(datasetPath, allFragmentIds));
        }

        ImmutableList.Builder<LanceSplit> splits = ImmutableList.builder();
        Set<Integer> covered = new LinkedHashSet<>();
        for (List<Integer> segmentFragments : segmentFragmentLists) {
            if (segmentFragments.isEmpty()) {
                continue;
            }
            splits.add(new LanceSplit(datasetPath, ImmutableList.copyOf(segmentFragments)));
            covered.addAll(segmentFragments);
        }

        List<Integer> uncovered = allFragmentIds.stream()
                .filter(id -> !covered.contains(id))
                .collect(toImmutableList());
        if (!uncovered.isEmpty()) {
            splits.add(new LanceSplit(datasetPath, uncovered));
        }

        List<LanceSplit> built = splits.build();
        if (built.isEmpty()) {
            return ImmutableList.of(new LanceSplit(datasetPath, allFragmentIds));
        }
        return built;
    }

    /**
     * Returns the segments of the vector index covering {@code column}, or an empty list when
     * the column has no index (callers then fall back to a single all-fragments split, which
     * Lance flat-searches).
     */
    static List<Index> findVectorIndexSegments(Dataset dataset, String column)
    {
        Optional<Integer> fieldId = dataset.getLanceSchema().fields().stream()
                .filter(field -> field.getName().equals(column))
                .map(LanceField::getId)
                .findFirst();
        if (!fieldId.isPresent()) {
            return ImmutableList.of();
        }
        for (IndexDescription description : dataset.describeIndices()) {
            if (description.getFieldIds().contains(fieldId.get())) {
                return description.getSegments();
            }
        }
        return ImmutableList.of();
    }

    /**
     * Extracts each segment's fragment coverage. Returns an empty list (single-split fallback)
     * when any segment lacks fragment metadata or two segments overlap — either would let the
     * same row reach the global top-k from two splits.
     */
    static List<List<Integer>> segmentFragmentLists(List<Index> segments)
    {
        ImmutableList.Builder<List<Integer>> lists = ImmutableList.builder();
        Set<Integer> seen = new HashSet<>();
        for (Index segment : segments) {
            if (!segment.fragments().isPresent()) {
                return ImmutableList.of();
            }
            for (Integer fragmentId : segment.fragments().get()) {
                if (!seen.add(fragmentId)) {
                    return ImmutableList.of();
                }
            }
            lists.add(ImmutableList.copyOf(segment.fragments().get()));
        }
        return lists.build();
    }
}
