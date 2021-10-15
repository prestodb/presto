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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestCustomSplitConversionUtils
{
    @Test
    public void testHudiRealtimeSplitConverterRoundTrip()
            throws IOException
    {
        Path expectedPath = new Path("s3://test/path");
        long expectedStart = 1L;
        long expectedLength = 2L;
        String[] expectedLocations = new String[] {"one", "two"};
        String expectedBasepath = "basepath";
        List<String> expectedDeltaLogPaths = Arrays.asList("test1", "test2", "test3");
        String expectedMaxCommitTime = "max_commit_time";

        FileSplit baseSplit = new FileSplit(expectedPath, expectedStart, expectedLength, expectedLocations);
        FileSplit hudiSplit = new HoodieRealtimeFileSplit(baseSplit, expectedBasepath, expectedDeltaLogPaths, expectedMaxCommitTime, Option.empty());

        // Test conversion of HudiSplit -> customSplitInfo
        Map<String, String> customSplitInfo = CustomSplitConversionUtils.extractCustomSplitInfo(hudiSplit);

        // Test conversion of (customSplitInfo + baseSplit) -> HudiSplit
        FileSplit recreatedSplit = CustomSplitConversionUtils.recreateSplitWithCustomInfo(baseSplit, customSplitInfo);

        assertEquals(recreatedSplit.getPath(), expectedPath);
        assertEquals(recreatedSplit.getStart(), expectedStart);
        assertEquals(recreatedSplit.getLength(), expectedLength);
        assertEquals(recreatedSplit.getLocations(), expectedLocations);
        assertEquals(((HoodieRealtimeFileSplit) recreatedSplit).getBasePath(), expectedBasepath);
        assertEquals(((HoodieRealtimeFileSplit) recreatedSplit).getDeltaLogPaths(), expectedDeltaLogPaths);
        assertEquals(((HoodieRealtimeFileSplit) recreatedSplit).getMaxCommitTime(), expectedMaxCommitTime);
    }
}
