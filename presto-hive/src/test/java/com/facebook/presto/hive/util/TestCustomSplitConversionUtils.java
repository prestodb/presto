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
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.realtime.RealtimeBootstrapBaseFileSplit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestCustomSplitConversionUtils
{
    private static final String BASE_PATH = "/test/table/";
    private static final Path FILE_PATH = new Path(BASE_PATH, "test.parquet");
    private static final long SPLIT_START_POS = 0L;
    private static final long SPLIT_LENGTH = 100L;
    private static final String[] SPLIT_HOSTS = new String[] {"host1", "host2"};

    @Test
    public void testHudiRealtimeSplitConverterRoundTrip() throws IOException
    {
        List<String> expectedDeltaLogPaths = Arrays.asList("test1", "test2", "test3");
        String expectedMaxCommitTime = "max_commit_time";

        FileSplit baseSplit = new FileSplit(FILE_PATH, SPLIT_START_POS, SPLIT_LENGTH, SPLIT_HOSTS);
        FileSplit hudiSplit = new HoodieRealtimeFileSplit(baseSplit, BASE_PATH, expectedDeltaLogPaths, expectedMaxCommitTime);

        // Test conversion of HudiSplit -> customSplitInfo
        Map<String, String> customSplitInfo = CustomSplitConversionUtils.extractCustomSplitInfo(hudiSplit);

        // Test conversion of (customSplitInfo + baseSplit) -> HudiSplit
        HoodieRealtimeFileSplit recreatedSplit = (HoodieRealtimeFileSplit) CustomSplitConversionUtils
                .recreateSplitWithCustomInfo(baseSplit, customSplitInfo);

        assertEquals(FILE_PATH, recreatedSplit.getPath());
        assertEquals(SPLIT_START_POS, recreatedSplit.getStart());
        assertEquals(SPLIT_LENGTH, recreatedSplit.getLength());
        assertEquals(SPLIT_HOSTS, recreatedSplit.getLocations());
        assertEquals(BASE_PATH, recreatedSplit.getBasePath());
        assertEquals(expectedDeltaLogPaths, recreatedSplit.getDeltaLogPaths());
        assertEquals(expectedMaxCommitTime, recreatedSplit.getMaxCommitTime());
    }

    @Test
    public void testHudiBootstrapBaseFileSplitConverter() throws IOException
    {
        Path bootstrapSourceFilePath = new Path("/test/source/test.parquet");
        long bootstrapSourceSplitStartPos = 0L;
        long bootstrapSourceSplitLength = 200L;

        FileSplit baseSplit = new FileSplit(FILE_PATH, SPLIT_START_POS, SPLIT_LENGTH, SPLIT_HOSTS);
        FileSplit bootstrapSourceSplit = new FileSplit(bootstrapSourceFilePath, bootstrapSourceSplitStartPos, bootstrapSourceSplitLength,
                new String[0]);
        FileSplit hudiSplit = new BootstrapBaseFileSplit(baseSplit, bootstrapSourceSplit);

        // Test conversion of HudiSplit -> customSplitInfo
        Map<String, String> customSplitInfo = CustomSplitConversionUtils.extractCustomSplitInfo(hudiSplit);

        // Test conversion of (customSplitInfo + baseSplit) -> HudiSplit
        BootstrapBaseFileSplit recreatedSplit = (BootstrapBaseFileSplit) CustomSplitConversionUtils
                .recreateSplitWithCustomInfo(baseSplit, customSplitInfo);

        assertEquals(FILE_PATH, recreatedSplit.getPath());
        assertEquals(SPLIT_START_POS, recreatedSplit.getStart());
        assertEquals(SPLIT_LENGTH, recreatedSplit.getLength());
        assertEquals(SPLIT_HOSTS, recreatedSplit.getLocations());
        assertEquals(bootstrapSourceFilePath, recreatedSplit.getBootstrapFileSplit().getPath());
        assertEquals(bootstrapSourceSplitStartPos, recreatedSplit.getBootstrapFileSplit().getStart());
        assertEquals(bootstrapSourceSplitLength, recreatedSplit.getBootstrapFileSplit().getLength());
    }

    @Test
    public void testHudiRealtimeBootstrapBaseFileSplitConverter() throws IOException
    {
        List<String> deltaLogPaths = Arrays.asList("test1", "test2", "test3");
        String maxCommitTime = "max_commit_time";

        Path bootstrapSourceFilePath = new Path("/test/source/test.parquet");
        long bootstrapSourceSplitStartPos = 0L;
        long bootstrapSourceSplitLength = 200L;

        FileSplit baseSplit = new FileSplit(FILE_PATH, SPLIT_START_POS, SPLIT_LENGTH, SPLIT_HOSTS);
        FileSplit bootstrapSourceSplit = new FileSplit(bootstrapSourceFilePath, bootstrapSourceSplitStartPos, bootstrapSourceSplitLength,
                new String[0]);
        FileSplit hudiSplit = new RealtimeBootstrapBaseFileSplit(baseSplit, BASE_PATH, deltaLogPaths, maxCommitTime,
                bootstrapSourceSplit);

        // Test conversion of HudiSplit -> customSplitInfo
        Map<String, String> customSplitInfo = CustomSplitConversionUtils.extractCustomSplitInfo(hudiSplit);

        // Test conversion of (customSplitInfo + baseSplit) -> HudiSplit
        RealtimeBootstrapBaseFileSplit recreatedSplit = (RealtimeBootstrapBaseFileSplit) CustomSplitConversionUtils
                .recreateSplitWithCustomInfo(baseSplit, customSplitInfo);

        assertEquals(FILE_PATH, recreatedSplit.getPath());
        assertEquals(SPLIT_START_POS, recreatedSplit.getStart());
        assertEquals(SPLIT_LENGTH, recreatedSplit.getLength());
        assertEquals(SPLIT_HOSTS, recreatedSplit.getLocations());
        assertEquals(BASE_PATH, recreatedSplit.getBasePath());
        assertEquals(deltaLogPaths, recreatedSplit.getDeltaLogPaths());
        assertEquals(maxCommitTime, recreatedSplit.getMaxCommitTime());
        assertEquals(bootstrapSourceFilePath, recreatedSplit.getBootstrapFileSplit().getPath());
        assertEquals(bootstrapSourceSplitStartPos, recreatedSplit.getBootstrapFileSplit().getStart());
        assertEquals(bootstrapSourceSplitLength, recreatedSplit.getBootstrapFileSplit().getLength());
    }
}
