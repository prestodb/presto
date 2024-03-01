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

import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for Split specific implementation of conversion from Split -> customSplitInfo Map and back.
 */
public interface CustomSplitConverter
{
    /**
     * This method is expected to return optional.empty() if the FileSplit does not match the split converter.
     */
    Optional<Map<String, String>> extractCustomSplitInfo(FileSplit split);

    /**
     * This method is expected to merge the customSplitInfo with split to recreate the custom FileSplit.
     * It is expected to return optional.empty() if the customSplitInfo does not match the split converter.
     */
    Optional<FileSplit> recreateFileSplitWithCustomInfo(FileSplit split, Map<String, String> customSplitInfo) throws IOException;
}
