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

import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.UnimplementedHiveMetastore;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;

public class TestingExtendedHiveMetastore
        extends UnimplementedHiveMetastore
{
    private final Map<String, Long> lastDataCommitTimes = new HashMap<>();
    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, Partition> partitions = new HashMap<>();

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return ImmutableList.of("hive_test");
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return Optional.of(new Database(databaseName, Optional.of("/"), "test_owner", PrincipalType.USER, Optional.empty(), ImmutableMap.of(), Optional.of("testcatalog")));
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        String tableKey = createTableKey(table.getDatabaseName(), table.getTableName());
        tables.put(tableKey, table);
        long currentTime = System.currentTimeMillis() / 1000;
        lastDataCommitTimes.put(tableKey, currentTime);

        return new MetastoreOperationResult(ImmutableList.of(currentTime));
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        String tableKey = createTableKey(databaseName, tableName);
        return Optional.ofNullable(tables.get(tableKey));
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        String tableKey = createTableKey(databaseName, tableName);
        lastDataCommitTimes.remove(tableKey);
        tables.remove(tableKey);
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        List<Long> times = new ArrayList<>();
        for (PartitionWithStatistics partition : partitions) {
            String partitionKey = createPartitionKey(databaseName, tableName, partition.getPartitionName());
            Partition oldPartition = this.partitions.put(partitionKey, partition.getPartition());

            if (oldPartition != null) {
                String oldLocation = oldPartition.getStorage().getLocation();
                String newLocation = partition.getPartition().getStorage().getLocation();

                // Use old data commit time if the location does not change.
                if (oldLocation.equals(newLocation)) {
                    times.add(lastDataCommitTimes.get(partitionKey));
                }
                else {
                    // Update the data commit time if the partition location changes.
                    // Adding 1000 to ensure their times are different since their data commit times are compared in seconds.
                    long currentTime = System.currentTimeMillis() / 1000;
                    lastDataCommitTimes.put(partitionKey, currentTime);
                    times.add(currentTime);
                }
            }
            else {
                // If the partition is new, add the data commit time.
                long currentTime = System.currentTimeMillis() / 1000;
                lastDataCommitTimes.put(partitionKey, currentTime);
                times.add(currentTime);
            }
        }
        return new MetastoreOperationResult(times);
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        String partitionKey = createPartitionKey(databaseName, tableName, partition.getPartitionName());
        Partition oldPartition = partitions.get(partitionKey);
        partitions.put(partitionKey, partition.getPartition());

        // When its location changes, we should generate a new commit time.
        if (oldPartition != null && oldPartition.getStorage().getLocation().equals(partition.getPartition().getStorage().getLocation())) {
            lastDataCommitTimes.put(partitionKey, System.currentTimeMillis() / 1000);
        }

        if (!lastDataCommitTimes.containsKey(partitionKey)) {
            return new MetastoreOperationResult(ImmutableList.of());
        }
        return new MetastoreOperationResult(ImmutableList.of(lastDataCommitTimes.get(partitionKey)));
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        String partitionKey = createPartitionKey(databaseName, tableName, partitionValues);
        long time = lastDataCommitTimes.getOrDefault(partitionKey, 0L);

        Partition partition = partitions.get(partitionKey);
        if (partition != null) {
            Partition.Builder builder = Partition.builder(partition)
                    .setLastDataCommitTime(time);
            return Optional.ofNullable(builder.build());
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNamesWithVersion)
    {
        Map<String, Optional<Partition>> result = new HashMap<>();
        for (PartitionNameWithVersion partitionNameWithVersion : partitionNamesWithVersion) {
            List<String> partitionValues = toPartitionValues(partitionNameWithVersion.getPartitionName());
            String partitionKey = createPartitionKey(databaseName, tableName, partitionValues);
            long time = lastDataCommitTimes.getOrDefault(partitionKey, 0L);

            Partition partition = partitions.get(partitionKey);
            if (partition != null) {
                Partition.Builder builder = Partition.builder(partition)
                        .setLastDataCommitTime(time);
                result.put(partitionNameWithVersion.getPartitionName(), Optional.of(builder.build()));
            }
            else {
                result.put(partitionNameWithVersion.getPartitionName(), Optional.empty());
            }
        }
        return result;
    }

    private String createPartitionKey(String databaseName, String tableName, String partitionName)
    {
        List<String> partitionValues = toPartitionValues(partitionName);

        return String.join(".", databaseName, tableName, partitionValues.toString());
    }

    private String createPartitionKey(String databaseName, String tableName, List<String> partitionValues)
    {
        return String.join(".", databaseName, tableName, partitionValues.toString());
    }

    private String createTableKey(String databaseName, String tableName)
    {
        return String.join(".", databaseName, tableName);
    }
}
