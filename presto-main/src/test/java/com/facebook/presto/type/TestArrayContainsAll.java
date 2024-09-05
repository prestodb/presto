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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestArrayContainsAll
        extends AbstractTestFunctions
{
    private static FunctionAssertions fieldNameInJsonCastEnabled;

    public TestArrayContainsAll() {}

    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
        fieldNameInJsonCastEnabled = new FunctionAssertions(
                Session.builder(session)
                        .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true")
                        .build(),
                new FeaturesConfig());
    }

    @AfterClass(alwaysRun = true)
    public final void tearDown()
    {
        fieldNameInJsonCastEnabled.close();
        fieldNameInJsonCastEnabled = null;
    }

    @Test
    public void testOverlappingArrays()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [2, 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [1, 2, 3])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [1, 1, 2, 3])", BooleanType.BOOLEAN, true);
    }

    @Test
    public void testDisjointArrays()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [4])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [4, 5])", BooleanType.BOOLEAN, false);
    }

    @Test
    public void testEmptyArrays()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [], ARRAY [])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [], ARRAY [1, 2, 3])", BooleanType.BOOLEAN, false);
    }

    @Test
    public void testDifferentDataTypes()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1.0, 2.0, 3.0], ARRAY [2.0])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1.0, 2.0, 3.0], ARRAY [4.0])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY ['a', 'b', 'c'], ARRAY ['b'])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY ['a', 'b', 'c'], ARRAY ['d'])", BooleanType.BOOLEAN, false);
    }

    @Test
    public void testNulls()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [NULL, 2, 3], ARRAY [2])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, NULL, 3], ARRAY [NULL])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1, 2, 3], ARRAY [NULL])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [NULL, NULL], ARRAY [NULL])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(NULL, ARRAY [1])", BooleanType.BOOLEAN, null);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY [1], NULL)", BooleanType.BOOLEAN, null);
        assertFunction("ARRAY_CONTAINS_ALL(NULL, NULL)", BooleanType.BOOLEAN, null);
    }

    @Test
    public void testArrayOfArrays()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[1, 2]])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[3, 4], ARRAY[5, 6]])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[5, 6]])", BooleanType.BOOLEAN, false);
    }

    @Test
    public void testArrayOfRows()
    {
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ROW(1, 'a'), ROW(2, 'b')], ARRAY[ROW(1, 'a')])", BooleanType.BOOLEAN, true);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ROW(1, 'a'), ROW(2, 'b')], ARRAY[ROW(2, 'b'), ROW(3, 'c')])", BooleanType.BOOLEAN, false);
        assertFunction("ARRAY_CONTAINS_ALL(ARRAY[ROW(1, 'a'), ROW(2, 'b')], ARRAY[ROW(3, 'c')])", BooleanType.BOOLEAN, false);
    }

    @Override
    public void assertInvalidFunction(String projection, SemanticErrorCode errorCode)
    {
        try {
            assertFunction(projection, UNKNOWN, null);
            fail("Expected error " + errorCode + " from " + projection);
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), errorCode);
        }
    }
}
