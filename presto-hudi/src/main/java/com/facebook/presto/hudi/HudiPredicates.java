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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HudiPredicates
{
    private final TupleDomain<HudiColumnHandle> partitionColumnPredicates;
    private final TupleDomain<HudiColumnHandle> regularColumnPredicates;

    public static HudiPredicates from(TupleDomain<ColumnHandle> predicate)
    {
        Map<HudiColumnHandle, Domain> partitionColumnPredicates = new HashMap<>();
        Map<HudiColumnHandle, Domain> regularColumnPredicates = new HashMap<>();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        domains.ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((key, value) -> {
            HudiColumnHandle columnHandle = (HudiColumnHandle) key;
            if (columnHandle.getColumnType() == HudiColumnHandle.ColumnType.PARTITION_KEY) {
                partitionColumnPredicates.put(columnHandle, value);
            }
            else {
                regularColumnPredicates.put(columnHandle, value);
            }
        }));

        return new HudiPredicates(
                TupleDomain.withColumnDomains(partitionColumnPredicates),
                TupleDomain.withColumnDomains(regularColumnPredicates));
    }

    private HudiPredicates(
            TupleDomain<HudiColumnHandle> partitionColumnPredicates,
            TupleDomain<HudiColumnHandle> regularColumnPredicates)
    {
        this.partitionColumnPredicates = partitionColumnPredicates;
        this.regularColumnPredicates = regularColumnPredicates;
    }

    public TupleDomain<HudiColumnHandle> getRegularColumnPredicates()
    {
        return regularColumnPredicates;
    }
}
