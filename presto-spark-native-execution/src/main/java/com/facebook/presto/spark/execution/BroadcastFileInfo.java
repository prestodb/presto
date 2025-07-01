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
package com.facebook.presto.spark.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * This class is a 1:1 strict API mapping to BroadcastFileInfo in
 * presto-native-execution/presto_cpp/main/operators/BroadcastFactory.h.
 * Please refrain changes to this API class. If any changes have to be made to
 * this class, one should make sure to make corresponding changes in the above
 * C++ struct and its corresponding serde functionalities.
 */
public class BroadcastFileInfo
{
    private final String filePath;
    // TODO: Add additional stats including checksum, num rows, size.

    @JsonCreator
    public BroadcastFileInfo(@JsonProperty("filePath") String filePath)
    {
        this.filePath = filePath;
    }

    @JsonProperty("filePath")
    public String getFilePath()
    {
        return filePath;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }
}
