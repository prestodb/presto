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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY;
import static java.util.Objects.requireNonNull;

/**
 * HoodieRealtimeFileSplit specific implementation of CustomSplitConverter.
 * Extracts customSplitInfo from HoodieRealtimeFileSplit and reconstructs HoodieRealtimeFileSplit from Map.
 */
public class HudiRealtimeSplitConverter
        implements CustomSplitConverter
{
    public static final String HUDI_DELTA_FILEPATHS_KEY = "hudi_delta_filepaths";
    public static final String HUDI_BASEPATH_KEY = "hudi_basepath";
    public static final String HUDI_MAX_COMMIT_TIME_KEY = "hudi_max_commit_time";

    @Override
    public Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split)
    {
        if (split instanceof HoodieRealtimeFileSplit) {
            HoodieRealtimeFileSplit hudiSplit = (HoodieRealtimeFileSplit) split;
            Map<String, String> customSplitInfo = ImmutableMap.<String, String>builder()
                    .put(CUSTOM_FILE_SPLIT_CLASS_KEY, HoodieRealtimeFileSplit.class.getName())
                    .put(HUDI_DELTA_FILEPATHS_KEY, String.join(",", hudiSplit.getDeltaLogPaths()))
                    .put(HUDI_BASEPATH_KEY, hudiSplit.getBasePath())
                    .put(HUDI_MAX_COMMIT_TIME_KEY, hudiSplit.getMaxCommitTime())
                    .build();
            return Optional.of(customSplitInfo);
        }
        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo) throws IOException
    {
        String customSplitClass = customSplitInfo.get(CUSTOM_FILE_SPLIT_CLASS_KEY);
        if (HoodieRealtimeFileSplit.class.getName().equals(customSplitClass)) {
            requireNonNull(customSplitInfo.get(HUDI_DELTA_FILEPATHS_KEY), "HUDI_DELTA_FILEPATHS_KEY is missing");
            List<String> deltaLogPaths = Arrays.asList(customSplitInfo.get(HUDI_DELTA_FILEPATHS_KEY).split(","));
            return Optional.of(new HoodieRealtimeFileSplit(
                split,
                requireNonNull(customSplitInfo.get(HUDI_BASEPATH_KEY), "HUDI_BASEPATH_KEY is missing"),
                deltaLogPaths,
                requireNonNull(customSplitInfo.get(HUDI_MAX_COMMIT_TIME_KEY), "HUDI_MAX_COMMIT_TIME_KEY is missing")));
        }
        return Optional.empty();
    }
}
