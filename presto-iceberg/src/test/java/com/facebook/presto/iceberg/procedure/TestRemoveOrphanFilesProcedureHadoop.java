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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toIcebergSchema;
import static com.google.common.io.Files.createTempDir;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.assertj.core.api.Fail.fail;
import static org.testng.Assert.assertEquals;

public class TestRemoveOrphanFilesProcedureHadoop
        extends TestRemoveOrphanFilesProcedureBase
{
    public TestRemoveOrphanFilesProcedureHadoop()
    {
        super(HADOOP, ImmutableMap.of());
    }

    @Test(dataProvider = "timezones")
    public void testRemoveOrphanFilesWithNonDefaultMetadataPath(String zoneId, boolean legacyTimestamp)
    {
        String tempTableName = "temp_test_table_with_specified_metadata_path";
        String tableTargetPath = createTempDir().toURI().toString();
        String specifiedMetadataPath = createTempDir().getAbsolutePath();

        // Hadoop doesn't support set table property `WRITE_METADATA_LOCATION` on table creation
        try {
            createTable(tempTableName, tableTargetPath, ImmutableMap.of(WRITE_METADATA_LOCATION, specifiedMetadataPath));
            fail("Should fail: Hadoop path-based tables cannot relocate metadata");
        }
        catch (IllegalArgumentException e) {
            assertEquals("Hadoop path-based tables cannot relocate metadata", e.getMessage());
        }
    }

    @Override
    Table createTable(String tableName, String targetPath, Map<String, String> tableProperties)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HADOOP.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.createTable(TableIdentifier.of(TEST_SCHEMA, tableName),
                toIcebergSchema(ImmutableList.of(ColumnMetadata.builder().setName("a").setType(INTEGER).build(),
                        ColumnMetadata.builder().setName("b").setType(VARCHAR).build())),
                null,
                tableProperties);
    }

    @Override
    Table loadTable(String tableName)
    {
        tableName = normalizeIdentifier(tableName, ICEBERG_CATALOG);
        Catalog catalog = CatalogUtil.loadCatalog(HADOOP.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    @Override
    void dropTableFromCatalog(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HADOOP.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), new Configuration());
        catalog.dropTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory(HADOOP);
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }
}
