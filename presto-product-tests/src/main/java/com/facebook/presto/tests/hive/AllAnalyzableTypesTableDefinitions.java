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
import java.util.concurrent.ThreadLocalRandom;

import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;

public final class AllAnalyzableTypesTableDefinitions
{
    private AllAnalyzableTypesTableDefinitions()
    {
    }

    @TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_ANALYZABLE_HIVE_TYPES_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setDataSource(getTextFileDataSource())
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> rowFormat)
    {
        return HiveTableDefinition.builder("all_analyzable_types")
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
//                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        (rowFormat.isPresent() ? "ROW FORMAT " + rowFormat.get() + " " : " ") +
                        "STORED AS " + fileFormat);
    }

    private static HiveDataSource getTextFileDataSource()
    {
        return createResourceDataSource("all_analyzable_types", String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), "com/facebook/presto/tests/hive/data/all_analyzable_types/data.textfile");
    }
}
