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
package io.prestosql.spi;

import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
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
}
