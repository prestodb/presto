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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;

@Test
public class TestIcebergDistributedHive
        extends IcebergDistributedTestBase
{
    public TestIcebergDistributedHive()
    {
        super(HIVE, ImmutableMap.of("iceberg.hive-statistics-merge-strategy", Joiner.on(",").join(NUMBER_OF_DISTINCT_VALUES.name(), TOTAL_SIZE_IN_BYTES.name())));
    }

    @Override
    public void testNDVsAtSnapshot()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testStatsByDistance()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testPartShowStatsWithFilters()
    {
        // Hive doesn't support returning statistics on partitioned tables
    }

    @Override
    public void testStatisticsFileCache()
            throws Exception
    {
        // hive doesn't write Iceberg statistics files when metastore is in use,
        // so this test won't complete successfully.
    }

    @Override
    protected Table loadTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }
}
