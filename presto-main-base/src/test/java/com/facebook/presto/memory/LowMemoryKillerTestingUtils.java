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

package com.facebook.presto.memory;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static io.airlift.units.DataSize.Unit.BYTE;

public class LowMemoryKillerTestingUtils
{
    private LowMemoryKillerTestingUtils() {}

    static List<MemoryInfo> toNodeMemoryInfoList(long maxReservedPoolBytes, long maxGeneralPoolBytes, String reservedQuery, Map<String, Map<String, Long>> queries)
    {
        Map<InternalNode, NodeReservation> nodeReservations = new HashMap<>();

        for (Map.Entry<String, Map<String, Long>> entry : queries.entrySet()) {
            QueryId queryId = new QueryId(entry.getKey());
            Map<String, Long> reservationByNode = entry.getValue();

            for (Map.Entry<String, Long> nodeEntry : reservationByNode.entrySet()) {
                InternalNode node = new InternalNode(nodeEntry.getKey(), URI.create("http://localhost"), new NodeVersion("version"), false);
                long bytes = nodeEntry.getValue();
                if (bytes == 0) {
                    continue;
                }
                if (reservedQuery.equals(entry.getKey())) {
                    nodeReservations.computeIfAbsent(node, ignored -> new NodeReservation()).getReserved().add(queryId, bytes);
                }
                else {
                    nodeReservations.computeIfAbsent(node, ignored -> new NodeReservation()).getGeneral().add(queryId, bytes);
                }
            }
        }

        ImmutableList.Builder<MemoryInfo> result = ImmutableList.builder();
        for (Map.Entry<InternalNode, NodeReservation> entry : nodeReservations.entrySet()) {
            NodeReservation nodeReservation = entry.getValue();
            ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> pools = ImmutableMap.builder();
            if (nodeReservation.getGeneral().getTotalReservedBytes() > 0) {
                pools.put(
                        GENERAL_POOL,
                        new MemoryPoolInfo(
                                maxGeneralPoolBytes,
                                nodeReservation.getGeneral().getTotalReservedBytes(),
                                0,
                                nodeReservation.getGeneral().getReservationByQuery(),
                                ImmutableMap.of(),
                                ImmutableMap.of()));
            }
            if (nodeReservation.getReserved().getTotalReservedBytes() > 0) {
                pools.put(
                        RESERVED_POOL,
                        new MemoryPoolInfo(
                                maxReservedPoolBytes,
                                nodeReservation.getReserved().getTotalReservedBytes(),
                                0,
                                nodeReservation.getReserved().getReservationByQuery(),
                                ImmutableMap.of(),
                                ImmutableMap.of()));
            }
            result.add(new MemoryInfo(new DataSize(maxReservedPoolBytes + maxGeneralPoolBytes, BYTE), pools.build()));
        }
        return result.build();
    }

    static List<LowMemoryKiller.QueryMemoryInfo> toQueryMemoryInfoList(String reservedQuery, Map<String, Map<String, Long>> queries)
    {
        ImmutableList.Builder<LowMemoryKiller.QueryMemoryInfo> result = ImmutableList.builder();
        for (Map.Entry<String, Map<String, Long>> entry : queries.entrySet()) {
            String queryId = entry.getKey();
            long totalReservation = entry.getValue().values().stream()
                    .mapToLong(x -> x)
                    .sum();
            result.add(new LowMemoryKiller.QueryMemoryInfo(new QueryId(queryId), queryId.equals(reservedQuery) ? RESERVED_POOL : GENERAL_POOL, totalReservation));
        }
        return result.build();
    }

    private static class NodeReservation
    {
        private final PoolReservation general = new PoolReservation();
        private final PoolReservation reserved = new PoolReservation();

        public PoolReservation getGeneral()
        {
            return general;
        }

        public PoolReservation getReserved()
        {
            return reserved;
        }
    }

    private static class PoolReservation
    {
        private long totalReservedBytes;
        private final Map<QueryId, Long> reservationByQuery = new HashMap<>();

        public void add(QueryId queryId, long bytes)
        {
            totalReservedBytes += bytes;
            reservationByQuery.put(queryId, bytes);
        }

        public long getTotalReservedBytes()
        {
            return totalReservedBytes;
        }

        public Map<QueryId, Long> getReservationByQuery()
        {
            return reservationByQuery;
        }
    }
}
