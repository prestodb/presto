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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;

import java.util.Optional;

public interface PagesSpatialIndex
{
    int[] findJoinPositions(int probePosition, Page probe, int probeGeometryChannel, Optional<Integer> probePartitionChannel);

    boolean isJoinPositionEligible(int joinPosition, int probePosition, Page probe);

    void appendTo(int joinPosition, PageBuilder pageBuilder, int outputChannelOffset);

    PagesSpatialIndex EMPTY_INDEX = new PagesSpatialIndex()
    {
        private final int[] emptyAddresses = new int[0];

        @Override
        public int[] findJoinPositions(int probePosition, Page probe, int probeGeometryChannel, Optional<Integer> probePartitionChannel)
        {
            return emptyAddresses;
        }

        @Override
        public boolean isJoinPositionEligible(int joinPosition, int probePosition, Page probe)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(int joinPosition, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }
    };
}
