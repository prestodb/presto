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

package com.facebook.presto.hudi.split;

import com.facebook.presto.spi.SplitWeight;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestSizeBasedSplitWeightProvider
{
    @Test
    public void testCalculateSplitWeight()
    {
        double minimumWeight = 0.2;
        DataSize standardSplitSize = new DataSize(100, MEGABYTE);
        HudiSplitWeightProvider weightProvider = new SizeBasedSplitWeightProvider(minimumWeight, standardSplitSize);
        assertWeight(weightProvider, 0, minimumWeight);
        assertWeight(weightProvider, 200 * 1024 * 1024, 1.0);
        assertWeight(weightProvider, 50 * 1024 * 1024, 0.5);
        assertWeight(weightProvider, 10 * 1024 * 1024, minimumWeight);
    }

    private void assertWeight(HudiSplitWeightProvider weightProvider, long splitSizeInBytes, double expectedWeight)
    {
        assertEquals(weightProvider.calculateSplitWeight(splitSizeInBytes), SplitWeight.fromProportion(expectedWeight));
    }
}
