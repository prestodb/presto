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
package io.prestosql.tests.datatype;

import com.google.common.base.Joiner;
import io.prestosql.tests.sql.SqlExecutor;
import io.prestosql.tests.sql.TestTable;

import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class CreateAsSelectDataSetup
        implements DataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    public CreateAsSelectDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        this.sqlExecutor = sqlExecutor;
        this.tableNamePrefix = tableNamePrefix;
    }

    @Override
    public TestTable setupTestTable(List<DataTypeTest.Input<?>> inputs)
    {
        List<String> columnValues = inputs.stream()
                .map(this::literalInExplicitCast)
                .collect(toList());
        Stream<String> columnValuesWithNames = range(0, columnValues.size())
                .mapToObj(i -> format("%s col_%d", columnValues.get(i), i));
        String selectBody = Joiner.on(",\n").join(columnValuesWithNames.iterator());
        String ddlTemplate = "CREATE TABLE {TABLE_NAME} AS SELECT\n" + selectBody;
        return new TestTable(sqlExecutor, tableNamePrefix, ddlTemplate);
    }

    private String literalInExplicitCast(DataTypeTest.Input<?> input)
    {
        return format("CAST(%s AS %s)", input.toLiteral(), input.getInsertType());
    }
}
