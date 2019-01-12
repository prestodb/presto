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

import io.prestosql.Session;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.sql.TestTable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class DataTypeTest
{
    private final List<Input<?>> inputs = new ArrayList<>();

    private DataTypeTest() {}

    public static DataTypeTest create()
    {
        return new DataTypeTest();
    }

    public <T> DataTypeTest addRoundTrip(DataType<T> dataType, T value)
    {
        inputs.add(new Input<>(dataType, value));
        return this;
    }

    public void execute(QueryRunner prestoExecutor, DataSetup dataSetup)
    {
        execute(prestoExecutor, prestoExecutor.getDefaultSession(), dataSetup);
    }

    public void execute(QueryRunner prestoExecutor, Session session, DataSetup dataSetup)
    {
        List<Type> expectedTypes = inputs.stream().map(Input::getPrestoResultType).collect(toList());
        List<Object> expectedResults = inputs.stream().map(Input::toPrestoQueryResult).collect(toList());
        try (TestTable testTable = dataSetup.setupTestTable(unmodifiableList(inputs))) {
            MaterializedResult materializedRows = prestoExecutor.execute(session, "SELECT * from " + testTable.getName());
            assertEquals(materializedRows.getTypes(), expectedTypes);
            List<Object> actualResults = getOnlyElement(materializedRows).getFields();
            assertEquals(actualResults.size(), expectedResults.size(), "lists don't have the same size");
            for (int i = 0; i < expectedResults.size(); i++) {
                assertEquals(actualResults.get(i), expectedResults.get(i), "Element " + i);
            }
        }
    }

    public static class Input<T>
    {
        private final DataType<T> dataType;
        private final T value;

        public Input(DataType<T> dataType, T value)
        {
            this.dataType = dataType;
            this.value = value;
        }

        String getInsertType()
        {
            return dataType.getInsertType();
        }

        Type getPrestoResultType()
        {
            return dataType.getPrestoResultType();
        }

        Object toPrestoQueryResult()
        {
            return dataType.toPrestoQueryResult(value);
        }

        String toLiteral()
        {
            return dataType.toLiteral(value);
        }
    }
}
