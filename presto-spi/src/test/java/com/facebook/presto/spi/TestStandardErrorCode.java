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

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_EXTERNAL;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStandardErrorCode
{
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
            assertLessThanOrEqual(code(errorCode), code(GENERIC_EXTERNAL));
        }
    }

    @Test
    public void testOrdering()
            throws Exception
    {
        Iterator<StandardErrorCode> iterator = asList(StandardErrorCode.values()).iterator();

        assertTrue(iterator.hasNext());
        int previous = code(iterator.next());

        while (iterator.hasNext()) {
            StandardErrorCode code = iterator.next();
            int current = code(code);
            assertGreaterThan(current, previous, "Code is out of order: " + code);
            if ((code != GENERIC_INTERNAL_ERROR) && (code != GENERIC_INSUFFICIENT_RESOURCES) && (code != GENERIC_EXTERNAL)) {
                assertEquals(current, previous + 1, "Code is not sequential: " + code);
            }
            previous = current;
        }
        assertEquals(previous, code(GENERIC_EXTERNAL), "Last code is not GENERIC_EXTERNAL");
    }

    private static int code(StandardErrorCode error)
    {
        return error.toErrorCode().getCode();
    }
}
