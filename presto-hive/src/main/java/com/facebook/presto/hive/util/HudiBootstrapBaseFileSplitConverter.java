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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public class HudiBootstrapBaseFileSplitConverter
        implements CustomSplitConverter
{
    public static final String BOOTSTRAP_FILE_SPLIT_PATH_KEY = "bootstrap_file_split_path";
    public static final String BOOTSTRAP_FILE_SPLIT_START_KEY = "bootstrap_split_start";
    public static final String BOOTSTRAP_FILE_SPLIT_LEN_KEY = "bootstrap_split_len";

    @Override
    public Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split)
    {
        if (split instanceof BootstrapBaseFileSplit) {
            ImmutableMap.Builder<String, String> customSplitInfo = new ImmutableMap.Builder<>();
            BootstrapBaseFileSplit hudiSplit = (BootstrapBaseFileSplit) split;
            customSplitInfo.put(CUSTOM_FILE_SPLIT_CLASS_KEY, BootstrapBaseFileSplit.class.getName());
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_PATH_KEY, hudiSplit.getBootstrapFileSplit().getPath().toString());
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_START_KEY, String.valueOf(hudiSplit.getBootstrapFileSplit().getStart()));
            customSplitInfo.put(BOOTSTRAP_FILE_SPLIT_LEN_KEY, String.valueOf(hudiSplit.getBootstrapFileSplit().getLength()));
            return Optional.of(customSplitInfo.build());
        }

        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo)
            throws IOException
    {
        requireNonNull(customSplitInfo);
        String customFileSplitClass = customSplitInfo.get(CUSTOM_FILE_SPLIT_CLASS_KEY);
        if (!isNullOrEmpty(customFileSplitClass) && BootstrapBaseFileSplit.class.getName().equals(customFileSplitClass)) {
            FileSplit bootstrapFileSplit = new FileSplit(
                    new Path(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_PATH_KEY)),
                    parseLong(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_START_KEY)),
                    parseLong(customSplitInfo.get(BOOTSTRAP_FILE_SPLIT_LEN_KEY)),
                    (String[]) null);
            split = new BootstrapBaseFileSplit(split, bootstrapFileSplit);
            return Optional.of(split);
        }
        return Optional.empty();
    }
}
