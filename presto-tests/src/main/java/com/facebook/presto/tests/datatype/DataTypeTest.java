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
package com.facebook.presto.tests.datatype;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.sql.TestTable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.testng.AssertJUnit.assertEquals;

public class DataTypeTest
{
    private List<Input<?>> inputs = new ArrayList<>();

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
        List<Type> expectedTypes = inputs.stream().map(Input::getPrestoResultType).collect(toList());
        List<Object> expectedResults = inputs.stream().map(Input::getValue).collect(toList());
        try (TestTable testTable = dataSetup.setupTestTable(unmodifiableList(inputs))) {
            MaterializedResult materializedRows = prestoExecutor.execute("SELECT * from " + testTable.getName());
            assertEquals(expectedTypes, materializedRows.getTypes());
            MaterializedRow row = getOnlyElement(materializedRows);
            assertEquals(expectedResults, row.getFields());
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

        Object getValue()
        {
            return value;
        }

        String toLiteral()
        {
            return dataType.toLiteral(value);
        }
    }
}
