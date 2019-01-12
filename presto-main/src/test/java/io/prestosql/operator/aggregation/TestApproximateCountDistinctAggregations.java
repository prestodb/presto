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
package io.prestosql.operator.aggregation;

import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import static io.prestosql.operator.aggregation.ApproximateCountDistinctAggregation.standardErrorToBuckets;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestApproximateCountDistinctAggregations
{
    @Test
    public void testStandardErrorToBuckets()
    {
        assertEquals(standardErrorToBuckets(0.0326), 1024);
        assertEquals(standardErrorToBuckets(0.0325), 1024);
        assertEquals(standardErrorToBuckets(0.0324), 2048);
        assertEquals(standardErrorToBuckets(0.0231), 2048);
        assertEquals(standardErrorToBuckets(0.0230), 2048);
        assertEquals(standardErrorToBuckets(0.0229), 4096);
        assertEquals(standardErrorToBuckets(0.0164), 4096);
        assertEquals(standardErrorToBuckets(0.0163), 4096);
        assertEquals(standardErrorToBuckets(0.0162), 8192);
        assertEquals(standardErrorToBuckets(0.0116), 8192);
        assertEquals(standardErrorToBuckets(0.0115), 8192);
        assertEquals(standardErrorToBuckets(0.0114), 16384);
        assertEquals(standardErrorToBuckets(0.008126), 16384);
        assertEquals(standardErrorToBuckets(0.008125), 16384);
        assertEquals(standardErrorToBuckets(0.008124), 32768);
        assertEquals(standardErrorToBuckets(0.00576), 32768);
        assertEquals(standardErrorToBuckets(0.00575), 32768);
        assertEquals(standardErrorToBuckets(0.00574), 65536);
        assertEquals(standardErrorToBuckets(0.0040626), 65536);
        assertEquals(standardErrorToBuckets(0.0040625), 65536);
    }

    @Test
    public void testStandardErrorToBucketsBounds()
    {
        try {
            // Lower bound
            standardErrorToBuckets(0.0040624);
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }

        try {
            // Upper bound
            standardErrorToBuckets(0.26001);
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }
    }
}
