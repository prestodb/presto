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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.List;

public interface DistributionDao
{
    @SqlUpdate("INSERT INTO distributions (distribution_id, distribution_name, bucket_count, column_types)\n" +
            "VALUES (:distributionId, :distributionName, :bucketCount, :columnTypes)")
    void createDistribution(
            @Bind long distributionId,
            @Bind byte[] distributionName,
            @Bind int bucketCount,
            @Bind byte[] columnTypes);

    @SqlBatch("INSERT INTO bucket_nodes (distribution_id, bucket_number, node_id)\n" +
            "VALUES (:distributionId, :bucketNumbers, :nodeIds)")
    void insertBucketNodes(
            @Bind long distributionId,
            @Bind List<Integer> bucketNumbers,
            @Bind List<Long> nodeIds);

    @SqlQuery("SELECT *\n" +
            "FROM distributions\n" +
            "WHERE distribution_id = :distributionId")
    DistributionInfo getDistributionInfo(
            @Bind long distributionId);

    @SqlQuery("SELECT *\n" +
            "FROM distributions\n" +
            "WHERE distribution_name = :distributionName")
    DistributionInfo getDistributionInfo(
            @Bind byte[] distributionName);

    @SqlQuery("SELECT bucket_number, node_id\n" +
            "FROM bucket_nodes\n" +
            "WHERE distribution_id = :distributionId")
    @UseRowMapper(BucketNode.Mapper.class)
    List<BucketNode> getBucketNodes(
            @Bind long distributionId);

    @SqlQuery("SELECT t.table_id, bn.bucket_number\n" +
            "FROM tables t\n" +
            "JOIN bucket_nodes bn ON t.distribution_id = bn.distribution_id\n" +
            "WHERE bn.node_id = :nodeId")
    @UseRowMapper(TableBucket.Mapper.class)
    List<TableBucket> getTableBuckets(
            @Bind long nodeId);
}
