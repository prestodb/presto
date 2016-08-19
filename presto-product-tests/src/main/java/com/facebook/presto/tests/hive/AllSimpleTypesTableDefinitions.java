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

import com.teradata.tempto.fulfillment.table.TableDefinitionsRepository;
import com.teradata.tempto.fulfillment.table.hive.HiveDataSource;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;

import java.util.Optional;

import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;

public final class AllSimpleTypesTableDefinitions
{
    // TODO generate data files using hive instead using prebuilt commited files.

    private AllSimpleTypesTableDefinitions()
    {
    }

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_TEXTFILE = allHiveSimpleTypesTableDefinition("TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"));

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_PARQUET = allHiveSimpleTypesParquetTableDefinition();

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_ORC = allHiveSimpleTypesTableDefinition("ORC", Optional.empty());

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_RCFILE = allHiveSimpleTypesTableDefinition("RCFILE", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"));

    private static HiveTableDefinition allHiveSimpleTypesTableDefinition(String fileFormat, Optional<String> rowFormat)
    {
        String tableName = fileFormat.toLowerCase() + "_all_types";
        HiveDataSource dataSource = createResourceDataSource(tableName, "" + System.currentTimeMillis(), "com/facebook/presto/tests/hive/data/all_types/data." + fileFormat.toLowerCase());
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE %EXTERNAL% TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        (rowFormat.isPresent() ? "ROW FORMAT " + rowFormat.get() + " " : " ") +
                        "STORED AS " + fileFormat)
                .setDataSource(dataSource)
                .build();
    }

    private static HiveTableDefinition allHiveSimpleTypesParquetTableDefinition()
    {
        String tableName = "parquet_all_types";
        HiveDataSource dataSource = createResourceDataSource(tableName, "" + System.currentTimeMillis(), "com/facebook/presto/tests/hive/data/all_types/data.parquet");
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE %EXTERNAL% TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "STORED AS PARQUET")
                .setDataSource(dataSource)
                .build();
    }

    private static HiveTableDefinition allHiveSimpleTypesKnownToPrestoTextfileTableDefinition()
    {
        String tableName = "textfile_all_types_known_to_presto";
        HiveDataSource dataSource = createResourceDataSource(tableName, "" + System.currentTimeMillis(), "com/facebook/presto/tests/hive/data/all_types_known_to_presto/data.textfile");
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE %EXTERNAL% TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "STORED AS TEXTFILE")
                .setDataSource(dataSource)
                .build();
    }
}
