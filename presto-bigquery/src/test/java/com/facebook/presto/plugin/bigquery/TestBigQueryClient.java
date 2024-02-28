package com.facebook.presto.plugin.bigquery;/*
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

import com.facebook.presto.Session;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertNotNull;

public class TestBigQueryClient
{
    private final BigQueryClient client;
    private final String projectId;

    public TestBigQueryClient()
            throws Exception
    {
        BigQueryConfig config = new BigQueryConfig()
                .setCredentialsKey("ckey")
                .setCredentialsFile("cfile")
                .setProjectId("pid")
                .setParentProjectId("ppid")
                .setParallelism(20)
                .setViewMaterializationProject("vmproject")
                .setViewMaterializationDataset("vmdataset")
                .setMaxReadRowsRetries(10);

        Session connector = BigQueryQueryRunner.createSession();
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder();
        client = new BigQueryClient(options.build().getService(), config);
        projectId = config.getProjectId().orElse(client.getProjectId());
    }
    @Test
    public void testCreateTableEmptyBigQueryColumn()
    {
        SchemaTableName tableName = new SchemaTableName("default", "test_create_table_empty_bigquery_column");

        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("a", BIGINT),
                new ColumnMetadata("b", BIGINT),
                new ColumnMetadata("c", BIGINT),
                new ColumnMetadata("d", BIGINT));

        Map<String, Object> properties = new HashMap<>();
        new BigQueryTableProperties().getTableProperties().forEach(meta -> properties.put(meta.getName(), meta.getDefaultValue()));
        properties.put("external", true);
        properties.put("column_mapping", "a:a:a,b::b,c:c:,d::");
        client.createTable(new ConnectorTableMetadata(tableName, columns, properties));
        TableInfo tableInfo = client.getTable(TableId.of(projectId, tableName.getSchemaName(), tableName.getTableName()));
        assertNotNull(client.getTable(tableInfo.getTableId()));
    }
}
