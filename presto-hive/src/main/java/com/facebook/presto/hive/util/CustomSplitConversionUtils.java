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

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;

/**
 * Utility class for both extracting customSplitInfo Map from a custom FileSplit and transforming the customSplitInfo back into a FileSplit.
 */
public class CustomSplitConversionUtils
{
    private static final List<CustomSplitConverter> converters = ImmutableList.of(
            new HudiRealtimeSplitConverter(),
            new HudiRealtimeBootstrapBaseFileSplitConverter(),
            new HudiBootstrapBaseFileSplitConverter());

    private CustomSplitConversionUtils()
    {
    }

    public static Map<String, String> extractCustomSplitInfo(FileSplit split)
    {
        for (CustomSplitConverter converter : converters) {
            Optional<Map<String, String>> customSplitData = converter.extractCustomSplitInfo(split);
            if (customSplitData.isPresent()) {
                return customSplitData.get();
            }
        }
        return ImmutableMap.of();
    }

    public static FileSplit recreateSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo)
    {
        for (CustomSplitConverter converter : converters) {
            Optional<FileSplit> fileSplit;
            try {
                fileSplit = converter.recreateFileSplitWithCustomInfo(split, customSplitInfo);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, String.format("Split converter %s failed to create FileSplit.", converter.getClass()), e);
            }
            if (fileSplit.isPresent()) {
                return fileSplit.get();
            }
        }
        return split;
    }
}
