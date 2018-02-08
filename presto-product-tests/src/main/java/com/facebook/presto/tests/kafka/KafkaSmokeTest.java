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
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.kafka.KafkaMessage;
import io.prestodb.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.prestodb.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.KAFKA;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestodb.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;

public class KafkaSmokeTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";

    private static final String SIMPLE_KEY_AND_VALUE_TABLE_NAME = "product_tests.simple_key_and_value";
    private static final String SIMPLE_KEY_AND_VALUE_TOPIC_NAME = "simple_key_and_value";

    // kafka-connectors requires tables to be predefined in presto configuration
    // the requirements here will be used to verify that table actually exists and to
    // create topics and propagate them with data

    private static class SimpleKeyAndValueTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    SIMPLE_KEY_AND_VALUE_TABLE_NAME,
                    SIMPLE_KEY_AND_VALUE_TOPIC_NAME,
                    new ListKafkaDataSource(ImmutableList.of(
                            new KafkaMessage(
                                    contentsBuilder().appendUTF8("jasio,1").build(),
                                    contentsBuilder().appendUTF8("ania,2").build()),
                            new KafkaMessage(
                                    contentsBuilder().appendUTF8("piotr,3").build(),
                                    contentsBuilder().appendUTF8("kasia,4").build()))),
                    1,
                    1));
        }
    }

    @Test(groups = {JDBC, KAFKA})
    @Requires(SimpleKeyAndValueTable.class)
    public void testSelectSimpleKeyAndValue()
    {
        QueryResult queryResult = query(format("select varchar_key, bigint_key, varchar_value, bigint_value from %s.%s", KAFKA_CATALOG, SIMPLE_KEY_AND_VALUE_TABLE_NAME));
        assertThat(queryResult).containsOnly(
                row("jasio", 1, "ania", 2),
                row("piotr", 3, "kasia", 4));
    }


    private static final String ALL_DATATYPES_RAW_TABLE_NAME = "product_tests.all_datatypes_raw";
    private static final String ALL_DATATYPES_RAW_TOPIC_NAME = "all_datatypes_raw";

    private static class AllDataTypesRawTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    ALL_DATATYPES_RAW_TABLE_NAME,
                    ALL_DATATYPES_RAW_TOPIC_NAME,
                    new ListKafkaDataSource(ImmutableList.of(
                            new KafkaMessage(
                                    contentsBuilder()
                                            .appendUTF8("jasio")
                                            .appendBytes(0x1)
                                            .appendBytes(0x2, 0x3)
                                            .appendBytes(0x4, 0x5, 0x6, 0x7)
                                            .appendBytes(0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf)
                                            .appendBytes(0x10)
                                            .appendBytes(0x11, 0x12)
                                            .appendBytes(0x13, 0x14, 0x15, 0x16)
                                            .appendBytes(0x17)
                                            .appendBytes(0x18, 0x19)
                                            .appendBytes(0x1a)
                                            .appendIntBigEndian(floatToIntBits(0.13f))
                                            .appendLongBigEndian(doubleToRawLongBits(0.45))
                                            .appendBytes(0x1b)
                                            .appendBytes(0x1c, 0x1d)
                                            .appendBytes(0x1e, 0x1f, 0x20, 0x21)
                                            .appendBytes(0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29)
                                            .build()))),
                    1,
                    1));
        }
    }

    @Test(groups = {JDBC, KAFKA})
    @Requires(AllDataTypesRawTable.class)
    public void testSelectAllRawTable()
    {
        QueryResult queryResult = query(format("select * from %s.%s", KAFKA_CATALOG, ALL_DATATYPES_RAW_TABLE_NAME));
        assertThat(queryResult).containsOnly(row(
                "jasio",
                0x01,
                0x0203,
                0x04050607,
                0x08090a0b0c0d0e0fL,
                0x10,
                0x1112,
                0x13141516,
                0x17,
                0x1819,
                0x1a,
                0.13f,
                0.45,
                true,
                true,
                true,
                true));
    }
}
