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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for fetching and updating column statistics in AWS Glue.
 * This component is responsible ONLY for interacting with AWS Glue APIs
 * and returns/accepts AWS Glue native statistics objects.
 * <p>
 * Conversion between Hive and Glue statistics formats is handled separately
 * by {@link com.facebook.presto.hive.metastore.glue.converter.GlueStatisticsConverter}.
 */
public interface GlueStatisticsFetcher
{
    /**
     * Fetch column statistics for a table from AWS Glue.
     *
     * @param table the table to fetch statistics for
     * @return list of AWS Glue ColumnStatistics objects
     */
    List<ColumnStatistics> getTableColumnStatistics(Table table);

    /**
     * Fetch column statistics for multiple partitions from AWS Glue.
     *
     * @param partitions the partitions to fetch statistics for
     * @return map of partition to list of AWS Glue ColumnStatistics objects
     */
    Map<Partition, List<ColumnStatistics>> getPartitionColumnStatistics(Set<Partition> partitions);

    /**
     * Update column statistics for a table in AWS Glue.
     * This will update existing statistics and delete any columns not in the provided list.
     *
     * @param table the table to update statistics for
     * @param columnStatistics list of AWS Glue ColumnStatistics to write
     */
    void updateTableColumnStatistics(Table table, List<ColumnStatistics> columnStatistics);

    /**
     * Update column statistics for multiple partitions in AWS Glue.
     * This will update existing statistics and delete any columns not in the provided lists.
     *
     * @param updates map of partition to list of AWS Glue ColumnStatistics to write
     */
    void updatePartitionColumnStatistics(Map<Partition, List<ColumnStatistics>> updates);
}
