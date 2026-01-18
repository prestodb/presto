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
package com.facebook.presto.common;

import org.testng.annotations.Test;

import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestErrorCode
{
    @Test
    public void testCatchableByTryFlag()
    {
        // Error code with catchableByTry = true
        ErrorCode catchable = new ErrorCode(1, "CATCHABLE_ERROR", USER_ERROR, false, true);
        assertTrue(catchable.isCatchableByTry());
        assertFalse(catchable.isRetriable());

        // Error code with catchableByTry = false
        ErrorCode notCatchable = new ErrorCode(2, "NOT_CATCHABLE_ERROR", USER_ERROR, false, false);
        assertFalse(notCatchable.isCatchableByTry());
    }

    @Test
    public void testBackwardCompatibleConstructorDefaultsToNotCatchable()
    {
        // 3-parameter constructor should default catchableByTry to false
        ErrorCode threeParam = new ErrorCode(1, "TEST_ERROR", USER_ERROR);
        assertFalse(threeParam.isCatchableByTry());
        assertFalse(threeParam.isRetriable());

        // 4-parameter constructor should default catchableByTry to false
        ErrorCode fourParam = new ErrorCode(2, "TEST_ERROR_2", USER_ERROR, true);
        assertFalse(fourParam.isCatchableByTry());
        assertTrue(fourParam.isRetriable());
    }

    @Test
    public void testErrorCodeProperties()
    {
        ErrorCode errorCode = new ErrorCode(123, "TEST_ERROR", INTERNAL_ERROR, true, true);

        assertEquals(errorCode.getCode(), 123);
        assertEquals(errorCode.getName(), "TEST_ERROR");
        assertEquals(errorCode.getType(), INTERNAL_ERROR);
        assertTrue(errorCode.isRetriable());
        assertTrue(errorCode.isCatchableByTry());
    }

    @Test
    public void testEquality()
    {
        // ErrorCode equality is based on code only (existing behavior)
        ErrorCode error1 = new ErrorCode(1, "ERROR_A", USER_ERROR, false, true);
        ErrorCode error2 = new ErrorCode(1, "ERROR_B", INTERNAL_ERROR, true, false);

        assertEquals(error1, error2);
        assertEquals(error1.hashCode(), error2.hashCode());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeCodeThrows()
    {
        new ErrorCode(-1, "NEGATIVE_CODE", USER_ERROR, false, false);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullNameThrows()
    {
        new ErrorCode(1, null, USER_ERROR, false, false);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullTypeThrows()
    {
        new ErrorCode(1, "TEST", null, false, false);
    }
}
