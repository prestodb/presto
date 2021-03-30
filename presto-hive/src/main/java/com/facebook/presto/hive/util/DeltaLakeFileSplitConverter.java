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

import io.delta.hive.DeltaInputSplit;
import io.delta.hive.PartitionColumnInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class DeltaLakeFileSplitConverter
        implements CustomSplitConverter
{
    public static final String CUSTOM_SPLIT_CLASS_KEY = "custom_split_class";
    public static final String FILE_SPLIT_PATH_KEY = "file_split_path";
    public static final String FILE_SPLIT_START_KEY = "split_start";
    public static final String FILE_SPLIT_LEN_KEY = "split_len";
    public static final String FILE_PARTITION_COLUMNS = "part_cols";

    @Override
    public Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split)
    {
        if (split instanceof DeltaInputSplit) {
            Map<String, String> customSplitInfo = new HashMap<>();
            DeltaInputSplit deltaInputSplit = (DeltaInputSplit) split;

            customSplitInfo.put(CUSTOM_SPLIT_CLASS_KEY, DeltaInputSplit.class.getName());
            customSplitInfo.put(FILE_SPLIT_PATH_KEY, deltaInputSplit.getPath().toString());
            customSplitInfo.put(FILE_SPLIT_START_KEY, String.valueOf(deltaInputSplit.getStart()));
            customSplitInfo.put(FILE_SPLIT_LEN_KEY, String.valueOf(deltaInputSplit.getLength()));
            String partCols = Arrays.stream(deltaInputSplit.getPartitionColumns())
                    .map(partitionColumnInfo ->
                            String.join(":",
                                    String.valueOf(partitionColumnInfo.index()),
                                    partitionColumnInfo.tpe(),
                                    partitionColumnInfo.value()))
                    .collect(Collectors.joining(","));
            customSplitInfo.put(FILE_PARTITION_COLUMNS, partCols);
            return Optional.of(customSplitInfo);
        }

        return Optional.empty();
    }

    @Override
    public Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo) throws IOException
    {
        requireNonNull(customSplitInfo);

        if (customSplitInfo.containsKey(CUSTOM_SPLIT_CLASS_KEY)
                && customSplitInfo.get(CUSTOM_SPLIT_CLASS_KEY).equals(DeltaInputSplit.class.getName())) {
            String partCols = customSplitInfo.get(FILE_PARTITION_COLUMNS);
            if (partCols == null || partCols.equals("")) {
                split = new DeltaInputSplit(
                        new Path(customSplitInfo.get(FILE_SPLIT_PATH_KEY)),
                        Long.parseLong(customSplitInfo.get(FILE_SPLIT_START_KEY)),
                        Long.parseLong(customSplitInfo.get(FILE_SPLIT_LEN_KEY)),
                        null, new PartitionColumnInfo[0]);
            }
            else {
                split = new DeltaInputSplit(
                        new Path(customSplitInfo.get(FILE_SPLIT_PATH_KEY)),
                        Long.parseLong(customSplitInfo.get(FILE_SPLIT_START_KEY)),
                        Long.parseLong(customSplitInfo.get(FILE_SPLIT_LEN_KEY)),
                        null,
                        Arrays.stream(customSplitInfo.get(FILE_PARTITION_COLUMNS)
                                .split(","))
                                .map(s -> {
                                    String[] ss = s.split(":");
                                    return new PartitionColumnInfo(
                                            Integer.parseInt(ss[0]),
                                            ss[1], ss[2]);
                                }).toArray(PartitionColumnInfo[]::new));
            }
            return Optional.of(split);
        }
        return Optional.empty();
    }
}
