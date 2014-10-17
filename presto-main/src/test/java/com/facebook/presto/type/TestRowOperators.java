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
package com.facebook.presto.type;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.type.SqlTimestamp;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;

public class TestRowOperators
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
        functionAssertions.getMetadata().getFunctionRegistry().addFunctions(new FunctionListBuilder(functionAssertions.getMetadata().getTypeManager()).scalar(TestingRowConstructor.class).getFunctions());
    }
    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testRowToJson()
            throws Exception
    {
        assertFunction("CAST(test_row(1, 2) AS JSON)", "[1,2]");
        assertFunction("CAST(test_row(1, CAST(NULL AS BIGINT)) AS JSON)", "[1,null]");
        assertFunction("CAST(test_row(1, 2.0) AS JSON)", "[1,2.0]");
        assertFunction("CAST(test_row(1.0, 2.5) AS JSON)", "[1.0,2.5]");
        assertFunction("CAST(test_row(1.0, 'kittens') AS JSON)", "[1.0,\"kittens\"]");
        assertFunction("CAST(test_row(TRUE, FALSE) AS JSON)", "[true,false]");
        assertFunction("CAST(test_row(from_unixtime(1)) AS JSON)", "[\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()).toString() + "\"]");
    }
}
