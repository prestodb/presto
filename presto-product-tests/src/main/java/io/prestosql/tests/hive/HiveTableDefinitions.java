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

import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;

import static io.prestodb.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static java.lang.String.format;

public class HiveTableDefinitions
{
    private static final String NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME = "nation_partitioned_by_regionkey";

    public static final int NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT = 5;

    public static final HiveTableDefinition NATION_PARTITIONED_BY_BIGINT_REGIONKEY =
            HiveTableDefinition.builder(NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME)
                    .setCreateTableDDLTemplate(
                            "CREATE %EXTERNAL% TABLE %NAME%(" +
                                    "   p_nationkey     BIGINT," +
                                    "   p_name          VARCHAR(25)," +
                                    "   p_comment       VARCHAR(152)) " +
                                    "PARTITIONED BY (p_regionkey BIGINT)" +
                                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ")
                    .addPartition(
                            "p_regionkey=1",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("bigint", "1")))
                    .addPartition(
                            "p_regionkey=2",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("bigint", "2")))
                    .addPartition(
                            "p_regionkey=3",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("bigint", "3")))
                    .build();

    public static final HiveTableDefinition NATION_PARTITIONED_BY_VARCHAR_REGIONKEY =
            HiveTableDefinition.builder(NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME)
                    .setCreateTableDDLTemplate(
                            "CREATE %EXTERNAL% TABLE %NAME%(" +
                                    "   p_nationkey     BIGINT," +
                                    "   p_name          VARCHAR(25)," +
                                    "   p_comment       VARCHAR(152)) " +
                                    "PARTITIONED BY (p_regionkey VARCHAR(20))" +
                                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ")
                    .addPartition(
                            "p_regionkey='AMERICA'",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("varchar", "america")))
                    .addPartition(
                            "p_regionkey='ASIA'",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("varchar", "asia")))
                    .addPartition(
                            "p_regionkey='EUROPE'",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    partitionDataFileResource("varchar", "europe")))
                    .build();

    private static String partitionDataFileResource(String key, String partition)
    {
        return format("com/facebook/presto/tests/hive/data/partitioned_nation_%s/nation_region_%s.textfile", key, partition);
    }

    private HiveTableDefinitions()
    {
    }
}
