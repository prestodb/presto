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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestMultisetOperators
        extends AbstractTestFunctions
{
    public TestMultisetOperators()
    {
        registerScalar(TestingRowConstructor.class);
    }

    @Test
    public void testMultisetToArrayCast()
            throws Exception
    {
        // first implementation uses array to represent multiset
        assertFunction("CAST(ARRAY[1,2,3] AS MULTISET<integer>)", new MultisetType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("CAST(CAST(ARRAY[1,2,3] AS MULTISET<integer>)  AS ARRAY<integer>)", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
    }

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

    public void assertInvalidFunction(String projection, ErrorCode errorCode)
    {
        try {
            assertFunction(projection, UNKNOWN, null);
            fail("Expected error " + errorCode + " from " + projection);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), errorCode);
        }
    }
}
