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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Result of fetching Partitions in the HivePartitionManager interface.
 *
 * Results are comprised of two parts:
 * 1) The actual partitions
 * 2) The TupleDomain that represents the values that the connector was not able to pre-evaluate
 * when generating the partitions and will need to be double-checked by the final execution plan.
 */
public class HivePartitionResult
{
    private final List<HiveColumnHandle> partitionColumns;
    private final List<HivePartition> partitions;
    private final TupleDomain<ColumnHandle> unenforcedConstraint;
    private final TupleDomain<ColumnHandle> enforcedConstraint;

    public HivePartitionResult(
            List<HiveColumnHandle> partitionColumns,
            List<HivePartition> partitions,
            TupleDomain<ColumnHandle> unenforcedConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
    }

    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    public List<HivePartition> getPartitions()
    {
        return partitions;
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }

    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }
}
