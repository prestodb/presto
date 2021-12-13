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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.spi.PrestoException;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.TestCachingHiveMetastore.MockHiveCluster;
import static java.util.Objects.requireNonNull;

public class MockHiveMetastore
        extends ThriftHiveMetastore
{
    private final MockHiveCluster clientProvider;

    public MockHiveMetastore(MockHiveCluster mockHiveCluster)
    {
        super(mockHiveCluster, new MetastoreClientConfig());
        this.clientProvider = requireNonNull(mockHiveCluster, "mockHiveCluster is null");
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(MetastoreContext context, String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        try {
            return clientProvider.createPartitionVersionSupportedMetastoreClient().getPartitionNamesWithVersionByFilter(databaseName, tableName, partitionPredicates);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
