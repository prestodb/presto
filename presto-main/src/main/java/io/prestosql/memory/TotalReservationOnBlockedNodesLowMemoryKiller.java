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

package io.prestosql.memory;

import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static java.util.Comparator.comparingLong;

public class TotalReservationOnBlockedNodesLowMemoryKiller
        implements LowMemoryKiller
{
    @Override
    public Optional<QueryId> chooseQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Map<QueryId, Long> memoryReservationOnBlockedNodes = new HashMap<>();
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo generalPool = node.getPools().get(GENERAL_POOL);
            if (generalPool == null) {
                continue;
            }
            if (generalPool.getFreeBytes() + generalPool.getReservedRevocableBytes() > 0) {
                continue;
            }
            Map<QueryId, Long> queryMemoryReservations = generalPool.getQueryMemoryReservations();
            queryMemoryReservations.forEach((queryId, memoryReservation) -> {
                memoryReservationOnBlockedNodes.compute(queryId, (id, oldValue) -> oldValue == null ? memoryReservation : oldValue + memoryReservation);
            });
        }

        return memoryReservationOnBlockedNodes.entrySet().stream()
                .max(comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey);
    }
}
