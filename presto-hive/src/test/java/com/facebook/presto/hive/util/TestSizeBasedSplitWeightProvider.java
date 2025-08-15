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
package com.facebook.presto.hive.util;

import com.facebook.presto.spi.SplitWeight;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSizeBasedSplitWeightProvider
{
    private static final long STANDARD_SPLIT_WEIGHT = SplitWeight.standard().getRawValue();

    @Test
    public void testSimpleProportions()
    {
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(0.01, DataSize.succinctBytes(megabytesToBytes(64)));
        assertEquals(provider.weightForSplitSizeInBytes(megabytesToBytes(64)), SplitWeight.fromRawValue(STANDARD_SPLIT_WEIGHT));
        assertEquals(provider.weightForSplitSizeInBytes(megabytesToBytes(32)), SplitWeight.fromRawValue(STANDARD_SPLIT_WEIGHT / 2));
        assertEquals(provider.weightForSplitSizeInBytes(megabytesToBytes(16)), SplitWeight.fromRawValue(STANDARD_SPLIT_WEIGHT / 4));
    }

    @Test
    public void testMinimumAndMaximumSplitWeightHandling()
    {
        DataSize targetSplitSize = DataSize.succinctBytes(megabytesToBytes(64));
        SizeBasedSplitWeightProvider provider = new SizeBasedSplitWeightProvider(0.05, targetSplitSize);
        assertEquals(provider.weightForSplitSizeInBytes(1), SplitWeight.fromRawValue(5));

        DataSize largerThanTarget = DataSize.succinctBytes(megabytesToBytes(128));
        assertEquals(provider.weightForSplitSizeInBytes(largerThanTarget.toBytes()), SplitWeight.fromRawValue(STANDARD_SPLIT_WEIGHT));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "^minimumWeight must be > 0 and <= 1, found: 1\\.01$")
    public void testInvalidMinimumWeight()
    {
        new SizeBasedSplitWeightProvider(1.01, DataSize.succinctBytes(megabytesToBytes(64)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "^targetSplitSize must be > 0, found:.*$")
    public void testInvalidTargetSplitSize()
    {
        new SizeBasedSplitWeightProvider(0.01, DataSize.succinctBytes(0));
    }

    private static long megabytesToBytes(int megabytes)
    {
        return ((long) megabytes) * 1024 * 1024;
    }
}
