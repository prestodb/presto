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

import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;

import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;

public class HiveTableDefinitions
{
    private static final String DATA_REVISION = "1";

    private static final String NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME = "nation_partitioned_by_regionkey";

    public static final int NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT = 5;

    public static final HiveTableDefinition NATION_PARTITIONED_BY_REGIONKEY =
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
                                    DATA_REVISION,
                                    partitionDataFileResource(1)))
                    .addPartition(
                            "p_regionkey=2",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    DATA_REVISION,
                                    partitionDataFileResource(2)))
                    .addPartition(
                            "p_regionkey=3",
                            createResourceDataSource(
                                    NATION_PARTITIONED_BY_REGIONKEY_TABLE_NAME,
                                    DATA_REVISION,
                                    partitionDataFileResource(3)))
                    .build();

    private static String partitionDataFileResource(int region)
    {
        return "com/facebook/presto/tests/hive/data/partitioned_nation/nation_region_" + region + ".textfile";
    }

    private HiveTableDefinitions()
    {
    }
}
