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
package com.facebook.presto.tests.hive;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;

import static com.facebook.presto.tests.TestGroups.AVRO;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static java.lang.String.format;

public class TestAvroSchemaUrl
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestAvroSchemaUrl/schemas");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("avro/original_schema.avsc")).openStream()) {
            hdfsClient.saveFile("/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc", inputStream);
        }
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestAvroSchemaUrl/schemas");
    }

    @DataProvider
    public Object[][] avroSchemaLocations()
    {
        return new Object[][] {
                {"file:///docker/volumes/presto-product-tests/avro/original_schema.avsc"}, // mounted in hadoop and presto containers
                {"hdfs://hadoop-master:9000/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"},
                {"hdfs:///user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"},
                {"/user/hive/warehouse/TestAvroSchemaUrl/schemas/original_schema.avsc"}, // `avro.schema.url` can actually be path on HDFS (not URL)
        };
    }

    @Test(dataProvider = "avroSchemaLocations", groups = {AVRO})
    public void testHiveCreatedTable(String schemaLocation)
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_hive");
        onHive().executeQuery(format("" +
                        "CREATE TABLE test_avro_schema_url_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.url'='%s')",
                schemaLocation));
        onHive().executeQuery("INSERT INTO test_avro_schema_url_hive VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_url_hive")).containsExactly(row("some text", 123042));
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_url_hive")).containsExactly(row("some text", 123042));

        onHive().executeQuery("DROP TABLE test_avro_schema_url_hive");
    }

    @Test(dataProvider = "avroSchemaLocations", groups = {AVRO})
    public void testPrestoCreatedTable(String schemaLocation)
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS test_avro_schema_url_presto");
        onPresto().executeQuery(format("CREATE TABLE test_avro_schema_url_presto (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_url='%s')", schemaLocation));
        onPresto().executeQuery("INSERT INTO test_avro_schema_url_presto VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_url_presto")).containsExactly(row("some text", 123042));

        onPresto().executeQuery("DROP TABLE test_avro_schema_url_presto");
    }
}
