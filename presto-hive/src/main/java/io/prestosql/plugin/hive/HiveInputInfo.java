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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiveInputInfo
{
    private final List<String> partitionIds;
    // Code that serialize HiveInputInfo into log would often need the ability to limit the length of log entries.
    // This boolean field allows such code to mark the log entry as length limited.
    private final boolean truncated;

    @JsonCreator
    public HiveInputInfo(
            @JsonProperty("partitionIds") List<String> partitionIds,
            @JsonProperty("truncated") boolean truncated)
    {
        this.partitionIds = partitionIds;
        this.truncated = truncated;
    }

    @JsonProperty
    public List<String> getPartitionIds()
    {
        return partitionIds;
    }

    @JsonProperty
    public boolean isTruncated()
    {
        return truncated;
    }
}
