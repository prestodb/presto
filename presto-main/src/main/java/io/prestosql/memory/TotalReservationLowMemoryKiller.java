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

import java.util.List;
import java.util.Optional;

import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;

public class TotalReservationLowMemoryKiller
        implements LowMemoryKiller
{
    @Override
    public Optional<QueryId> chooseQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        QueryId biggestQuery = null;
        long maxMemory = 0;
        for (QueryMemoryInfo query : runningQueries) {
            long bytesUsed = query.getMemoryReservation();
            if (bytesUsed > maxMemory && GENERAL_POOL.equals(query.getMemoryPoolId())) {
                biggestQuery = query.getQueryId();
                maxMemory = bytesUsed;
            }
        }
        return Optional.ofNullable(biggestQuery);
    }
}
