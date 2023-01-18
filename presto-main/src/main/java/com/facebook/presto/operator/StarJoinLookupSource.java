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
package com.facebook.presto.operator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;

public class StarJoinLookupSource
{
    public static final long NO_MATCH = -1;
    public static final long NOT_INITIALIZED = -2;
    private final PartitionedLookupSource[] lookupSources;
    private final long[][][] pointerToNextSource;
    private final int size;

    public StarJoinLookupSource(List<LookupSource> lookupSources)
    {
        checkArgument(lookupSources.stream().allMatch(x -> x instanceof PartitionedLookupSource));
        this.lookupSources = new PartitionedLookupSource[lookupSources.size()];
        for (int i = 0; i < lookupSources.size(); ++i) {
            this.lookupSources[i] = (PartitionedLookupSource) lookupSources.get(i);
        }
        this.pointerToNextSource = new long[lookupSources.size()][][];
        for (int i = 0; i < lookupSources.size(); ++i) {
            int partitionCount = this.lookupSources[0].getPartitionCount();
            long maxPartitionSize = this.lookupSources[i].getMaxPartitionJoinPositionCount();
            long[][] pointers = new long[partitionCount][toIntExact(maxPartitionSize)];
            for (int j = 0; j < partitionCount; ++j) {
                Arrays.fill(pointers[j], NOT_INITIALIZED);
            }
            this.pointerToNextSource[i] = pointers;
        }
        this.size = lookupSources.size();
    }

    // For join index at position, get its match position for join index+1
    public long getNextStarPosition(int index, long position, JoinProbe probe)
    {
        if (position == -1) {
            return probe.getCurrentJoinPosition(lookupSources[index + 1]);
        }
        int partition = lookupSources[index].decodePartition(position);
        long positionInPartition = lookupSources[index].decodeJoinPosition(position);
        long next = pointerToNextSource[index][partition][toIntExact(positionInPartition)];
        if (next == NOT_INITIALIZED) {
            next = probe.getCurrentJoinPosition(lookupSources[index + 1]);
            pointerToNextSource[index][partition][toIntExact(positionInPartition)] = next;
        }
        return next;
    }
}
