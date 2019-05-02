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
package com.facebook.presto.tests.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.kafka.KafkaMessage;
import io.prestodb.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.prestodb.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.prestodb.tempto.query.QueryResult;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.Map;

import static com.facebook.presto.tests.TestGroups.KAFKA;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestodb.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class KafkaAvroSmokeTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";

    private static final String ALL_DATATYPES_AVRO_TABLE_NAME = "product_tests.all_datatypes_avro";
    private static final String ALL_DATATYPES_AVRO_TOPIC_NAME = "all_datatypes_avro";
    private static final String ALL_DATATYPE_SCHEMA_PATH = "/docker/volumes/conf/presto/etc/catalog/kafka/all_datatypes_avro_schema.avsc";

    private static final String ALL_NULL_AVRO_TABLE_NAME = "product_tests.all_null_avro";
    private static final String ALL_NULL_AVRO_TOPIC_NAME = "all_null_avro";

    private static final String STRUCTURAL_AVRO_TABLE_NAME = "product_tests.structural_datatype_avro";
    private static final String STRUCTURAL_AVRO_TOPIC_NAME = "structural_datatype_avro";
    private static final String STRUCTURAL_SCHEMA_PATH = "/docker/volumes/conf/presto/etc/catalog/kafka/structural_datatype_avro_schema.avsc";

    // kafka-connectors requires tables to be predefined in presto configuration
    // the requirements here will be used to verify that table actually exists and to
    // create topics and propagate them with data
    private static class AllDataTypesAvroTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            ImmutableMap<String, Object> record = ImmutableMap.of(
                    "a_varchar", "foobar",
                    "a_bigint", 127L,
                    "a_double", 234.567,
                    "a_boolean", true);
            return createAvroTable(ALL_DATATYPE_SCHEMA_PATH, ALL_DATATYPES_AVRO_TABLE_NAME, ALL_DATATYPES_AVRO_TOPIC_NAME, record);
        }
    }

    private static Requirement createAvroTable(String schemaPath, String tableName, String topicName, ImmutableMap<String, Object> record)
    {
        try {
            Schema schema = new Schema.Parser().parse(new File(schemaPath));
            byte[] avroData = convertRecordToAvro(schema, record);

            return immutableTable(new KafkaTableDefinition(
                    tableName,
                    topicName,
                    new ListKafkaDataSource(ImmutableList.of(
                            new KafkaMessage(
                                    contentsBuilder()
                                            .appendBytes(avroData)
                                            .build()))),
                    1,
                    1));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] convertRecordToAvro(Schema schema, Map<String, Object> values)
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericData.Record record = new GenericData.Record(schema);
        values.forEach(record::put);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
            dataFileWriter.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to convert to Avro.", e);
        }
        return outputStream.toByteArray();
    }

    @Test(groups = {KAFKA})
    @Requires(AllDataTypesAvroTable.class)
    public void testSelectPrimitiveDataType()
            throws SQLException
    {
        QueryResult queryResult = query(format("select * from %s.%s", KAFKA_CATALOG, ALL_DATATYPES_AVRO_TABLE_NAME));
        assertThat(queryResult).containsOnly(row(
                "foobar",
                127,
                234.567,
                true));
    }

    private static class NullDataAvroTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return createAvroTable(ALL_DATATYPE_SCHEMA_PATH, ALL_NULL_AVRO_TABLE_NAME, ALL_NULL_AVRO_TOPIC_NAME, ImmutableMap.of());
        }
    }

    @Test(groups = {KAFKA})
    @Requires(NullDataAvroTable.class)
    public void testNullType()
            throws SQLException
    {
        QueryResult queryResult = query(format("select * from %s.%s", KAFKA_CATALOG, ALL_NULL_AVRO_TABLE_NAME));
        assertThat(queryResult).containsOnly(row(
                null,
                null,
                null,
                null));
    }

    private static class StructuralDataTypeTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            ImmutableMap<String, Object> record = ImmutableMap.of(
                    "a_array", ImmutableList.of(100L, 102L),
                    "a_map", ImmutableMap.of("key1", "value1"));
            return createAvroTable(STRUCTURAL_SCHEMA_PATH, STRUCTURAL_AVRO_TABLE_NAME, STRUCTURAL_AVRO_TOPIC_NAME, record);
        }
    }

    @Test(groups = {KAFKA})
    @Requires(StructuralDataTypeTable.class)
    public void testSelectStructuralDataType()
            throws SQLException
    {
        QueryResult queryResult = query(format("SELECT a[1], a[2], m['key1'] FROM (SELECT c_array as a, c_map as m FROM %s.%s) t", KAFKA_CATALOG, STRUCTURAL_AVRO_TABLE_NAME));
        assertThat(queryResult).containsOnly(row(100, 102, "value1"));
    }
}
