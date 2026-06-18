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
package com.facebook.presto.spi;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertLessThan;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStandardErrorCode
{
    private static final int EXTERNAL_ERROR_START = 0x0100_0000;

    @Test
    public void testUnique()
    {
        Set<Integer> codes = new HashSet<>();
        for (StandardErrorCode code : StandardErrorCode.values()) {
            assertTrue(codes.add(code(code)), "Code already exists: " + code);
        }
        assertEquals(codes.size(), StandardErrorCode.values().length);
    }

    @Test
    public void testReserved()
    {
        for (StandardErrorCode errorCode : StandardErrorCode.values()) {
            assertLessThan(code(errorCode), EXTERNAL_ERROR_START);
        }
    }

    @Test
    public void testOrdering()
    {
        Iterator<StandardErrorCode> iterator = asList(StandardErrorCode.values()).iterator();

        assertTrue(iterator.hasNext());
        int previous = code(iterator.next());

        while (iterator.hasNext()) {
            StandardErrorCode code = iterator.next();
            int current = code(code);
            assertGreaterThan(current, previous, "Code is out of order: " + code);
            if ((code != GENERIC_INTERNAL_ERROR) && (code != GENERIC_INSUFFICIENT_RESOURCES)) {
                assertEquals(current, previous + 1, "Code is not sequential: " + code);
            }
            previous = current;
        }
    }

    private static int code(StandardErrorCode error)
    {
        return error.toErrorCode().getCode();
    }

    @Test
    public void testCatchableByTryErrors()
    {
        // These errors should be caught by TRY() and return NULL
        assertTrue(DIVISION_BY_ZERO.toErrorCode().isCatchableByTry());
        assertTrue(INVALID_CAST_ARGUMENT.toErrorCode().isCatchableByTry());
        assertTrue(INVALID_FUNCTION_ARGUMENT.toErrorCode().isCatchableByTry());
        assertTrue(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().isCatchableByTry());
    }

    @Test
    public void testNonCatchableByTryErrors()
    {
        // These errors should NOT be caught by TRY() - they should propagate
        assertFalse(GENERIC_INTERNAL_ERROR.toErrorCode().isCatchableByTry());
        assertFalse(GENERIC_USER_ERROR.toErrorCode().isCatchableByTry());
        assertFalse(SYNTAX_ERROR.toErrorCode().isCatchableByTry());
        assertFalse(GENERIC_INSUFFICIENT_RESOURCES.toErrorCode().isCatchableByTry());
    }
}
