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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.TableInfo;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.createOrganizationSet;
import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizerUtil.getChunksByDaysBuckets;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class CompactionSetCreator
{
    private final DataSize maxChunkSize;
    private final long maxChunkRows;
    private final TemporalFunction temporalFunction;

    public CompactionSetCreator(TemporalFunction temporalFunction, DataSize maxChunkSize, long maxChunkRows)
    {
        checkArgument(maxChunkRows > 0, "maxChunkRows must be > 0");

        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        this.maxChunkSize = requireNonNull(maxChunkSize, "maxChunkSize is null");
        this.maxChunkRows = maxChunkRows;
    }

    // Expects a pre-filtered collection of chunks.
    // All chunks provided to this method will be considered for creating a compaction set.
    public Set<OrganizationSet> createCompactionSets(TableInfo tableInfo, Collection<ChunkIndexInfo> chunks)
    {
        Collection<Collection<ChunkIndexInfo>> chunksByDaysBuckets = getChunksByDaysBuckets(tableInfo, chunks, temporalFunction);

        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();
        for (Collection<ChunkIndexInfo> chunkInfos : chunksByDaysBuckets) {
            compactionSets.addAll(buildCompactionSets(tableInfo, ImmutableSet.copyOf(chunkInfos)));
        }
        return compactionSets.build();
    }

    private Set<OrganizationSet> buildCompactionSets(TableInfo tableInfo, Set<ChunkIndexInfo> chunkIndexInfos)
    {
        List<ChunkIndexInfo> chunks = chunkIndexInfos.stream()
                .sorted(getChunkIndexInfoComparator(tableInfo))
                .collect(toCollection(ArrayList::new));

        long consumedBytes = 0;
        long consumedRows = 0;
        ImmutableSet.Builder<ChunkIndexInfo> builder = ImmutableSet.builder();
        ImmutableSet.Builder<OrganizationSet> compactionSets = ImmutableSet.builder();

        for (ChunkIndexInfo chunk : chunks) {
            if (((consumedBytes + chunk.getUncompressedSize()) > maxChunkSize.toBytes()) ||
                    (consumedRows + chunk.getRowCount() > maxChunkRows)) {
                // Finalize this compaction set, and start a new one for the rest of the chunks
                Set<ChunkIndexInfo> chunksToCompact = builder.build();

                if (chunksToCompact.size() > 1) {
                    compactionSets.add(createOrganizationSet(tableInfo, chunksToCompact));
                }

                builder = ImmutableSet.builder();
                consumedBytes = 0;
                consumedRows = 0;
            }
            builder.add(chunk);
            consumedBytes += chunk.getUncompressedSize();
            consumedRows += chunk.getRowCount();
        }

        // create compaction set for the remaining chunks of this day
        Set<ChunkIndexInfo> chunksToCompact = builder.build();
        if (chunksToCompact.size() > 1) {
            compactionSets.add(createOrganizationSet(tableInfo, chunksToCompact));
        }
        return compactionSets.build();
    }

    private static Comparator<ChunkIndexInfo> getChunkIndexInfoComparator(TableInfo tableInfo)
    {
        if (!tableInfo.getTemporalColumnId().isPresent()) {
            return comparing(ChunkIndexInfo::getUncompressedSize);
        }

        return comparing(info -> info.getTemporalRange().get(),
                comparing(ChunkRange::getMinTuple)
                        .thenComparing(ChunkRange::getMaxTuple));
    }
}
