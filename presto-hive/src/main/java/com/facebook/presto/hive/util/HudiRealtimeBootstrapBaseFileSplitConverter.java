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

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.realtime.RealtimeBootstrapBaseFileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY;
import static java.util.Objects.requireNonNull;

public final class HudiRealtimeBootstrapBaseFileSplitConverter
        implements CustomSplitConverter
{
    private static final String DELTA_FILE_PATHS_KEY = "delta_file_paths";
    private static final String BASE_PATH_KEY = "base_path";
    private static final String MAX_COMMIT_TIME_KEY = "max_commit_time";
    private static final String BOOTSTRAP_FILE_SPLIT_PATH = "bootstrap_split_path";
    private static final String BOOTSTRAP_FILE_SPLIT_START = "bootstrap_split_start";
    private static final String BOOTSTRAP_FILE_SPLIT_LEN = "bootstrap_split_len";

    @Override
    public Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split)
    {
        if (split instanceof RealtimeBootstrapBaseFileSplit) {
            Map<String, String> customSplitInfo = new HashMap<>();
            RealtimeBootstrapBaseFileSplit hudiSplit = (RealtimeBootstrapBaseFileSplit) split;

            customSplitInfo.put(CUSTOM_FILE_SPLIT_CLASS_KEY, RealtimeBootstrapBaseFileSplit.class.getName());
            customSplitInfo.put(BASE_PATH_KEY, hudiSplit.getBasePath());
            customSplitInfo.put(MAX_COMMIT_TIME_KEY, hudiSplit.getMaxCommitTime());
            customSplitInfo.put(DELTA_FILE_PATHS_KEY, String.join(",", hudiSplit.getDeltaLogPaths()));
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_PATH, hudiSplit.getBootstrapFileSplit().getPath().toString());
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_START, String.valueOf(hudiSplit.getBootstrapFileSplit().getStart()));
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_LEN, String.valueOf(hudiSplit.getBootstrapFileSplit().getLength()));
            return Optional.of(customSplitInfo);
        }
        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo)
            throws IOException
    {
        requireNonNull(customSplitInfo);

        if (customSplitInfo.containsKey(CUSTOM_FILE_SPLIT_CLASS_KEY)
                && customSplitInfo.get(CUSTOM_FILE_SPLIT_CLASS_KEY).equals(RealtimeBootstrapBaseFileSplit.class.getName())) {
            List<String> deltaLogPaths = Strings.isNullOrEmpty(customSplitInfo.get(DELTA_FILE_PATHS_KEY)) ?
                    Collections.emptyList() : Arrays.asList(customSplitInfo.get(DELTA_FILE_PATHS_KEY).split(","));
            FileSplit bootstrapFileSplit = new FileSplit(
                    new Path(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_PATH)),
                    Long.parseLong(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_START)),
                    Long.parseLong(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_LEN)),
                    (String[]) null);
            split = new RealtimeBootstrapBaseFileSplit(split, customSplitInfo.get(BASE_PATH_KEY), deltaLogPaths,
                    customSplitInfo.get(MAX_COMMIT_TIME_KEY), bootstrapFileSplit);
            return Optional.of(split);
        }
        return Optional.empty();
    }
}
