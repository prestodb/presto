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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.spi.ColumnHandle;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HivePartitionMetadata
{
    private final Optional<Partition> partition;
    private final HivePartition hivePartition;
    private final Map<Integer, Column> partitionSchemaDifference;
    private final Optional<EncryptionInformation> encryptionInformation;
    // This is a set of columns whose domain could be removed from table layout because all of the value in the partition will satisfy.
    private final Set<ColumnHandle> redundantColumnDomains;

    HivePartitionMetadata(
            HivePartition hivePartition,
            Optional<Partition> partition,
            Map<Integer, Column> partitionSchemaDifference,
            Optional<EncryptionInformation> encryptionInformation,
            Set<ColumnHandle> redundantColumnDomains)
    {
        this.partition = requireNonNull(partition, "partition is null");
        this.hivePartition = requireNonNull(hivePartition, "hivePartition is null");
        this.partitionSchemaDifference = requireNonNull(partitionSchemaDifference, "partitionSchemaDifference is null");
        this.encryptionInformation = requireNonNull(encryptionInformation, "encryptionInformation is null");
        this.redundantColumnDomains = requireNonNull(redundantColumnDomains, "redundantColumnDomains is null");
    }

    public HivePartition getHivePartition()
    {
        return hivePartition;
    }

    /**
     * @return empty if this HivePartitionMetadata represents an unpartitioned table
     */
    public Optional<Partition> getPartition()
    {
        return partition;
    }

    public Map<Integer, Column> getPartitionSchemaDifference()
    {
        return partitionSchemaDifference;
    }

    public Optional<EncryptionInformation> getEncryptionInformation()
    {
        return encryptionInformation;
    }

    public Set<ColumnHandle> getRedundantColumnDomains()
    {
        return redundantColumnDomains;
    }
}
