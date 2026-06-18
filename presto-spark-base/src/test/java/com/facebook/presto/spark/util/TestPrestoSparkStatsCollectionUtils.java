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
package com.facebook.presto.spark.util;

import org.testng.annotations.Test;

import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.getSparkInternalAccumulatorKey;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkStatsCollectionUtils
{
    @Test
    public void getSparkInternalAccumulatorKeyInternalKeyTest()
    {
        String expected = "internal.metrics.appname.writerRejectedPackageRawBytes";
        String prestoKey = "ShuffleWrite.root.internal.metrics.appname.writerRejectedPackageRawBytes";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }

    @Test
    public void getSparkInternalAccumulatorKeyTest()
    {
        String expected = "internal.metrics.velox.TableScanBlockedWaitForSplitTimes";
        String prestoKey = "TableScan.0.BlockedWaitForSplitTimes";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }

    @Test
    public void getSparkInternalAccumulatorKeyUnsupportedTest()
    {
        String expected = "";
        String prestoKey = "UnknownFormat";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }
}
