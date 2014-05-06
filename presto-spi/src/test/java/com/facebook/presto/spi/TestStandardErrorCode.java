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
import java.util.Set;

import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStandardErrorCode
{
    @Test
    public void testUnique()
    {
        Set<Integer> codes = new HashSet<>();
        for (StandardErrorCode code : StandardErrorCode.values()) {
            assertTrue(codes.add(code.toErrorCode().getCode()), "Code already exists: " + code);
        }
        assertEquals(codes.size(), StandardErrorCode.values().length);
    }

    @Test
    public void testReserved()
    {
        for (StandardErrorCode errorCode : StandardErrorCode.values()) {
            assertLessThanOrEqual(errorCode.toErrorCode().getCode(), StandardErrorCode.EXTERNAL.toErrorCode().getCode());
        }
    }
}
