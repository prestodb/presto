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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

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
    private static final String SCHEMA_NAME = "product_tests";

    private static final String SIMPLE_KEY_AND_VALUE_TABLE_NAME = "simple_key_and_value";
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
                    SCHEMA_NAME + "." + SIMPLE_KEY_AND_VALUE_TABLE_NAME,
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

    @Test(groups = {KAFKA})
    @Requires(SimpleKeyAndValueTable.class)
    public void testSelectSimpleKeyAndValue()
    {
        QueryResult queryResult = query(format(
                "select varchar_key, bigint_key, varchar_value, bigint_value from %s.%s.%s",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                SIMPLE_KEY_AND_VALUE_TABLE_NAME));
        assertThat(queryResult).containsOnly(
                row("jasio", 1, "ania", 2),
                row("piotr", 3, "kasia", 4));
    }

    private static final String ALL_DATATYPES_RAW_TABLE_NAME = "all_datatypes_raw";
    private static final String ALL_DATATYPES_RAW_TOPIC_NAME = "all_datatypes_raw";

    private static class AllDataTypesRawTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    SCHEMA_NAME + "." + ALL_DATATYPES_RAW_TABLE_NAME,
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

    @Test(groups = {KAFKA})
    @Requires(AllDataTypesRawTable.class)
    public void testSelectAllRawTable()
    {
        assertThat(query(format(
                "select column_name,data_type from %s.information_schema.columns where table_schema='%s' and table_name='%s'",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_RAW_TABLE_NAME
        ))).containsOnly(
                row("c_varchar", "varchar"),
                row("c_byte_bigint", "bigint"),
                row("c_short_bigint", "bigint"),
                row("c_int_bigint", "bigint"),
                row("c_long_bigint", "bigint"),
                row("c_byte_integer", "integer"),
                row("c_short_integer", "integer"),
                row("c_int_integer", "integer"),
                row("c_byte_smallint", "smallint"),
                row("c_short_smallint", "smallint"),
                row("c_byte_tinyint", "tinyint"),
                row("c_float_double", "double"),
                row("c_double_double", "double"),
                row("c_byte_boolean", "boolean"),
                row("c_short_boolean", "boolean"),
                row("c_int_boolean", "boolean"),
                row("c_long_boolean", "boolean"));

        assertThat(query(format("select * from %s.%s.%s", KAFKA_CATALOG, SCHEMA_NAME, ALL_DATATYPES_RAW_TABLE_NAME))).containsOnly(row(
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

    private static final String ALL_DATATYPES_CSV_TABLE_NAME = "all_datatypes_csv";
    private static final String ALL_DATATYPES_CSV_TOPIC_NAME = "all_datatypes_csv";

    private static class AllDataTypesCsvTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    SCHEMA_NAME + "." + ALL_DATATYPES_CSV_TABLE_NAME,
                    ALL_DATATYPES_CSV_TOPIC_NAME,
                    new ListKafkaDataSource(ImmutableList.of(
                            utf8KafkaMessage("jasio,9223372036854775807,2147483647,32767,127,1234567890.123456789,true"),
                            utf8KafkaMessage("stasio,-9223372036854775808,-2147483648,-32768,-128,-1234567890.123456789,blah"),
                            utf8KafkaMessage(",,,,,,"),
                            utf8KafkaMessage("krzysio,9223372036854775807,2147483647,32767,127,1234567890.123456789,false,extra,fields"),
                            utf8KafkaMessage("kasia,9223372036854775807,2147483647,32767"))),
                    1,
                    1));
        }
    }

    @Test(groups = {KAFKA})
    @Requires(AllDataTypesCsvTable.class)
    public void testSelectAllCsvTable()
    {
        assertThat(query(format(
                "select column_name,data_type from %s.information_schema.columns where table_schema='%s' and table_name='%s'",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_CSV_TABLE_NAME
        ))).containsOnly(
                row("c_varchar", "varchar"),
                row("c_bigint", "bigint"),
                row("c_integer", "integer"),
                row("c_smallint", "smallint"),
                row("c_tinyint", "tinyint"),
                row("c_double", "double"),
                row("c_boolean", "boolean"));

        assertThat(query(format("select * from %s.%s.%s", KAFKA_CATALOG, SCHEMA_NAME, ALL_DATATYPES_CSV_TABLE_NAME))).containsOnly(
                row("jasio", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, true),
                row("stasio", -9223372036854775808L, -2147483648, -32768, -128, -1234567890.123456789, false),
                row(null, null, null, null, null, null, null),
                row("krzysio", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, false),
                row("kasia", 9223372036854775807L, 2147483647, 32767, null, null, null));
    }

    private static final String ALL_DATATYPES_JSON_TABLE_NAME = "all_datatypes_json";
    private static final String ALL_DATATYPES_JSON_TOPIC_NAME = "all_datatypes_json";

    private static class AllDataTypesJsonTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(new KafkaTableDefinition(
                    SCHEMA_NAME + "." + ALL_DATATYPES_JSON_TABLE_NAME,
                    ALL_DATATYPES_JSON_TOPIC_NAME,
                    new ListKafkaDataSource(ImmutableList.of(
                            utf8KafkaMessage("{" +
                                    "\"j_varchar\"                              : \"ala ma kota\"                    ," +
                                    "\"j_bigint\"                               : \"9223372036854775807\"            ," +
                                    "\"j_integer\"                              : \"2147483647\"                     ," +
                                    "\"j_smallint\"                             : \"32767\"                          ," +
                                    "\"j_tinyint\"                              : \"127\"                            ," +
                                    "\"j_double\"                               : \"1234567890.123456789\"           ," +
                                    "\"j_boolean\"                              : \"true\"                           ," +
                                    "\"j_timestamp_milliseconds_since_epoch\"   : \"1518182116000\"                  ," +
                                    "\"j_timestamp_seconds_since_epoch\"        : \"1518182117\"                     ," +
                                    "\"j_timestamp_iso8601\"                    : \"2018-02-09T13:15:18\"            ," +
                                    "\"j_timestamp_rfc2822\"                    : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                                    "\"j_timestamp_custom\"                     : \"02/2018/09 13:15:20\"            ," +
                                    "\"j_date_iso8601\"                         : \"2018-02-11\"                     ," +
                                    "\"j_date_rfc2822\"                         : \"Mon Feb 12 13:15:16 Z 2018\"     ," +
                                    "\"j_date_custom\"                          : \"2018/13/02\"                     ," +
                                    "\"j_time_milliseconds_since_epoch\"        : \"47716000\"                       ," +
                                    "\"j_time_seconds_since_epoch\"             : \"47717\"                          ," +
                                    "\"j_time_iso8601\"                         : \"13:15:18\"                       ," +
                                    "\"j_time_rfc2822\"                         : \"Thu Jan 01 13:15:19 Z 1970\"     ," +
                                    "\"j_time_custom\"                          : \"15:13:20\"                       ," +
                                    "\"j_timestamptz_milliseconds_since_epoch\" : \"1518182116000\"                  ," +
                                    "\"j_timestamptz_seconds_since_epoch\"      : \"1518182117\"                     ," +
                                    "\"j_timestamptz_iso8601\"                  : \"2018-02-09T13:15:18Z\"           ," +
                                    "\"j_timestamptz_rfc2822\"                  : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                                    "\"j_timestamptz_custom\"                   : \"02/2018/09 13:15:20\"            ," +
                                    "\"j_timetz_milliseconds_since_epoch\"      : \"47716000\"                       ," +
                                    "\"j_timetz_seconds_since_epoch\"           : \"47717\"                          ," +
                                    "\"j_timetz_iso8601\"                       : \"13:15:18Z\"                      ," +
                                    "\"j_timetz_rfc2822\"                       : \"Thu Jan 01 13:15:19 Z 1970\"     ," +
                                    "\"j_timetz_custom\"                        : \"15:13:20\"                       }"))),
                    1,
                    1));
        }
    }

    @Test(groups = {KAFKA})
    @Requires(AllDataTypesJsonTable.class)
    public void testSelectAllJsonTable()
    {
        assertThat(query(format(
                "select column_name,data_type from %s.information_schema.columns where table_schema='%s' and table_name='%s'",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_JSON_TABLE_NAME
        ))).containsOnly(
                row("c_varchar", "varchar"),
                row("c_bigint", "bigint"),
                row("c_integer", "integer"),
                row("c_smallint", "smallint"),
                row("c_tinyint", "tinyint"),
                row("c_double", "double"),
                row("c_boolean", "boolean"),
                row("c_timestamp_milliseconds_since_epoch", "timestamp"),
                row("c_timestamp_seconds_since_epoch", "timestamp"),
                row("c_timestamp_iso8601", "timestamp"),
                row("c_timestamp_rfc2822", "timestamp"),
                row("c_timestamp_custom", "timestamp"),
                row("c_date_iso8601", "date"),
                row("c_date_rfc2822", "date"),
                row("c_date_custom", "date"),
                row("c_time_milliseconds_since_epoch", "time"),
                row("c_time_seconds_since_epoch", "time"),
                row("c_time_iso8601", "time"),
                row("c_time_rfc2822", "time"),
                row("c_time_custom", "time"),
                row("c_timestamptz_milliseconds_since_epoch", "timestamp with time zone"),
                row("c_timestamptz_seconds_since_epoch", "timestamp with time zone"),
                row("c_timestamptz_iso8601", "timestamp with time zone"),
                row("c_timestamptz_rfc2822", "timestamp with time zone"),
                row("c_timestamptz_custom", "timestamp with time zone"),
                row("c_timetz_milliseconds_since_epoch", "time with time zone"),
                row("c_timetz_seconds_since_epoch", "time with time zone"),
                row("c_timetz_iso8601", "time with time zone"),
                row("c_timetz_rfc2822", "time with time zone"),
                row("c_timetz_custom", "time with time zone"));

        assertThat(query(format("select * from %s.%s.%s", KAFKA_CATALOG, SCHEMA_NAME, ALL_DATATYPES_JSON_TABLE_NAME))).containsOnly(row(
                "ala ma kota",
                9223372036854775807L,
                2147483647,
                32767,
                127,
                1234567890.123456789,
                true,
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 16)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 17)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 18)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 19)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 20)),
                Date.valueOf(LocalDate.of(2018, 2, 11)),
                Date.valueOf(LocalDate.of(2018, 2, 12)),
                Date.valueOf(LocalDate.of(2018, 2, 13)),
                Time.valueOf(LocalTime.of(18, 45, 16)), // different due to broken TIME datatype semantics
                Time.valueOf(LocalTime.of(18, 45, 17)),
                Time.valueOf(LocalTime.of(18, 45, 18)),
                Time.valueOf(LocalTime.of(18, 45, 19)),
                Time.valueOf(LocalTime.of(18, 45, 20)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 16)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 17)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 18)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 19)),
                Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 20)),
                Time.valueOf(LocalTime.of(18, 45, 16)), // different due to broken TIME datatype semantics
                Time.valueOf(LocalTime.of(18, 45, 17)),
                Time.valueOf(LocalTime.of(18, 45, 18)),
                Time.valueOf(LocalTime.of(18, 45, 19)),
                Time.valueOf(LocalTime.of(18, 45, 20))));
    }

    private static KafkaMessage utf8KafkaMessage(String val)
    {
        return new KafkaMessage(contentsBuilder().appendUTF8(val).build());
    }
}
