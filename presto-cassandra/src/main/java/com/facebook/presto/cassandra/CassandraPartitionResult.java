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
package com.facebook.presto.cassandra;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;

import java.util.List;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Objects.requireNonNull;

public class CassandraPartitionResult
{
    private final List<CassandraPartition> partitions;
    private final TupleDomain<ColumnHandle> unenforcedConstraint;

    public CassandraPartitionResult(List<CassandraPartition> partitions, TupleDomain<ColumnHandle> unenforcedConstraint)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
    }

    public List<CassandraPartition> getPartitions()
    {
        return partitions;
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }

    public boolean isUnpartitioned()
    {
        return partitions.size() == 1 && partitions.stream().collect(onlyElement()).isUnpartitioned();
    }
}
