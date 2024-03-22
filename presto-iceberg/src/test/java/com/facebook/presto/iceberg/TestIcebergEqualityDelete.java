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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.parquet.ParquetReaderUtils.getParquetFileRowCount;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergEqualityDelete
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestIcebergEqualityDelete.class);
    private final CatalogType catalogType = CatalogType.HIVE;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of("http-server.http.port", "8081"), catalogType);
    }

    private void createTestTables()
    {
        Session session = Session.builder(getSession())
                .setSchema("tpcds").build();

        assertUpdate(session, "CREATE TABLE store_sales AS " +
                "SELECT * FROM tpcds.tiny.store_sales", 120527);
        assertQuerySucceeds(session, "CREATE TABLE date_dim AS " +
                "SELECT d_date_sk, cast(d_date_id as varchar) as d_date_id, d_date, " +
                "   d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, " +
                "   d_fy_quarter_seq, d_fy_week_seq, cast(d_day_name as varchar) as d_day_name, cast(d_quarter_name as varchar) as d_quarter_name, " +
                "   cast(d_holiday as varchar) as d_holiday,  cast(d_weekend as varchar) as d_weekend, " +
                "   cast(d_following_holiday as varchar) as d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,  " +
                "   cast(d_current_day as varchar) as d_current_day, cast(d_current_week as varchar) as d_current_week, " +
                "   cast(d_current_month as varchar) as d_current_month,  cast(d_current_quarter as varchar) as d_current_quarter, " +
                "   cast(d_current_year as varchar) as d_current_year " +
                "FROM tpcds.tiny.date_dim");

        assertUpdate(session, "CREATE TABLE DF_SS_1 as " +
                        "SELECT * " +
                        "FROM store_sales " +
                        "WHERE  ss_sold_date_sk >= (SELECT Min(d_date_sk) " +
                        "                           FROM   date_dim " +
                        "                           WHERE  d_date BETWEEN date'2000-05-20' AND date'2000-05-21') " +
                        "       AND ss_sold_date_sk <= (SELECT Max(d_date_sk) " +
                        "                               FROM   date_dim " +
                        "                               WHERE  d_date BETWEEN date'2000-05-20' AND " +
                        "                                                     date'2000-05-21') ",
                64L);
    }

    @Test(enabled = false)
    public void testEqualityDelete() throws IOException
    {
        createTestTables();

        long queryCount = (long) computeScalar("SELECT COUNT(coalesce(ss_ext_discount_amt,0)) FROM iceberg.tpcds.store_sales WHERE (ss_quantity BETWEEN 1 AND 20)"); // +
        log.info("Query result count " + queryCount);

        long expectedCount = (long) computeScalar("SELECT COUNT(coalesce(ss_ext_discount_amt,0)) FROM iceberg.tpcds.store_sales WHERE (ss_quantity BETWEEN 1 AND 20)" +
                " AND ss_sold_date_sk NOT IN (SELECT ss_sold_date_sk from iceberg.tpcds.DF_SS_1)");
        log.info("Post Delete count using NOT IN is " + expectedCount);

        addDeleteFiles("STORE_SALES", "DF_SS_1", "ss_sold_date_sk", "tpcds");

        long actualCount = (long) computeScalar("SELECT COUNT(coalesce(ss_ext_discount_amt,0)) FROM iceberg.tpcds.store_sales WHERE (ss_quantity BETWEEN 1 AND 20)");
        log.info("Post Delete count using Equality Delete is " + actualCount);
        assertEquals(actualCount, expectedCount);
    }

    private void addDeleteFiles(String tableName, String deleteTableName, String deleteColumnName, String schema)
            throws IOException
    {
        String tableLocation = getLocation("catalog", tableName, schema);
        System.out.println("Table location " + tableLocation);
        String deleteTableLocation = getLocation("catalog", deleteTableName, schema);

        File[] files = ((new File(deleteTableLocation + "/data")).listFiles((dir, name) -> name.endsWith(".parquet")));
        assertTrue(files != null);

        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        Table icebergTable = getIcebergTable(getSession().toConnectorSession(connectorId), "tpcds", tableName);
        Transaction transaction = icebergTable.newTransaction();

        RowDelta rowDelta = transaction.newRowDelta();
        for (File deleteFilePath : files) {
            System.out.println("Delete File location " + deleteFilePath);

            File dest = new File(tableLocation + "/data/delete_file_" + deleteFilePath.getName());

            Files.copy(deleteFilePath.toPath(), dest.toPath());

            long rowCount = getParquetFileRowCount(dest);
            CommitTaskData task = new CommitTaskData(dest.getPath(),
                    dest.length(),
                    new MetricsWrapper(rowCount,
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of(),
                            ImmutableMap.of()),
                    0,
                    Optional.empty(),
                    FileFormat.PARQUET, null);

            PartitionSpec spec = icebergTable.specs().get(0);
            int fieldId = icebergTable.schema().findField(deleteColumnName).fieldId();

            FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec)
                    .ofEqualityDeletes(fieldId)
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat("parquet")
                    .withMetrics(task.getMetrics().metrics());
            rowDelta.addDeletes(builder.build());
        }
        rowDelta.commit();
        transaction.commitTransaction();
    }

    protected String getLocation(String catalog, String table, String schema)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s/%s/%s/%s", tempLocation.getPath(), catalog, schema, table);
    }

    protected static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    protected Path getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory().resolve("catalog");
        return dataDirectory;
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toFile().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        TestIcebergEqualityDelete equalityDeletes = new TestIcebergEqualityDelete();
        equalityDeletes.init();
        equalityDeletes.createTestTables();
        equalityDeletes.addDeleteFiles("STORE_SALES", "DF_SS_1", "ss_sold_date_sk", "tpcds");

        Thread.sleep(10);
        Logger log = Logger.get(IcebergQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", equalityDeletes.getDistributedQueryRunner().getCoordinator().getBaseUrl());
    }
}
