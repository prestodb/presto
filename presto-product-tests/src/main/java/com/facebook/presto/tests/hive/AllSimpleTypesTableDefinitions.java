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

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static java.lang.String.format;

public final class AllSimpleTypesTableDefinitions
{
    // TODO generate data files using hive instead using prebuilt commited files.

    private AllSimpleTypesTableDefinitions()
    {
    }

    private static String tableNameFormat = "%s_all_types";

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setDataSource(getTextFileDataSource())
            .build();

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_PARQUET = parquetTableDefinitionBuilder()
            .setDataSource(getParquetDataSource())
            .build();

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_ORC = tableDefinitionBuilder("ORC", Optional.empty())
            .setDataSource(getOrcDataSource())
            .build();

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_RCFILE = tableDefinitionBuilder("RCFILE", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setDataSource(getRcfileDataSource())
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> rowFormat)
    {
        String tableName = format(tableNameFormat, fileFormat.toLowerCase(Locale.ENGLISH));
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE %EXTERNAL% TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        (rowFormat.isPresent() ? "ROW FORMAT " + rowFormat.get() + " " : " ") +
                        "STORED AS " + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder parquetTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("parquet_all_types")
                .setCreateTableDDLTemplate("" +
                        "CREATE %EXTERNAL% TABLE %NAME%(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "STORED AS PARQUET");
    }

    private static HiveDataSource getTextFileDataSource()
    {
        return createResourceDataSource(format(tableNameFormat, "textfile"), String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/all_types/data.textfile");
    }

    private static HiveDataSource getParquetDataSource()
    {
        return createResourceDataSource(format(tableNameFormat, "parquet"), String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/all_types/data.parquet");
    }

    private static HiveDataSource getOrcDataSource()
    {
        return createResourceDataSource(format(tableNameFormat, "orc"), String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/all_types/data.orc");
    }

    private static HiveDataSource getRcfileDataSource()
    {
        return createResourceDataSource(format(tableNameFormat, "rcfile"), String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/all_types/data.rcfile");
    }
}
