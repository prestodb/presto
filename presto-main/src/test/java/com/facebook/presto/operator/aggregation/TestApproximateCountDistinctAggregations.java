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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.PrestoException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations.standardErrorToBuckets;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TestApproximateCountDistinctAggregations
{
    @Test
    public void testStandardErrorToBuckets()
            throws Exception
    {
        Assert.assertEquals(standardErrorToBuckets(0.0326), 1024);
        Assert.assertEquals(standardErrorToBuckets(0.0325), 1024);
        Assert.assertEquals(standardErrorToBuckets(0.0324), 2048);
        Assert.assertEquals(standardErrorToBuckets(0.0231), 2048);
        Assert.assertEquals(standardErrorToBuckets(0.0230), 2048);
        Assert.assertEquals(standardErrorToBuckets(0.0229), 4096);
        Assert.assertEquals(standardErrorToBuckets(0.0164), 4096);
        Assert.assertEquals(standardErrorToBuckets(0.0163), 4096);
        Assert.assertEquals(standardErrorToBuckets(0.0162), 8192);
        Assert.assertEquals(standardErrorToBuckets(0.0116), 8192);
        Assert.assertEquals(standardErrorToBuckets(0.0115), 8192);

    }

    @Test
    public void testStandardErrorToBucketsBounds()
            throws Exception
    {
        try {
            // Lower bound
            standardErrorToBuckets(0.01149);
            Assert.fail();
        }
        catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }

        try {
            // Upper bound
            standardErrorToBuckets(0.26001);
            Assert.fail();
        }
        catch (PrestoException e) {
            Assert.assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }
    }
}
