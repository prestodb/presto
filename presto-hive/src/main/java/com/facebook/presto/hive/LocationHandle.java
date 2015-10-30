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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LocationHandle
{
    private final Path targetPath;
    private final Optional<Path> writePath;
    private final boolean isExistingTable;

    public LocationHandle(
            Path targetPath,
            Optional<Path> writePath,
            boolean isExistingTable)
    {
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.isExistingTable = isExistingTable;
    }

    @JsonCreator
    public LocationHandle(
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("writePath") Optional<String> writePath,
            @JsonProperty("isExistingTable") boolean isExistingTable)
    {
        this.targetPath = new Path(requireNonNull(targetPath, "targetPath is null"));
        this.writePath = requireNonNull(writePath, "writePath is null").map(Path::new);
        this.isExistingTable = isExistingTable;
    }

    // This method should only be called by LocationService
    Path getTargetPath()
    {
        return targetPath;
    }

    // This method should only be called by LocationService
    Optional<Path> getWritePath()
    {
        return writePath;
    }

    // This method should only be called by LocationService
    boolean isExistingTable()
    {
        return isExistingTable;
    }

    @JsonProperty("targetPath")
    public String getJsonSerializableTargetPath()
    {
        return targetPath.toString();
    }

    @JsonProperty("writePath")
    public Optional<String> getJsonSerializableWritePath()
    {
        return writePath.map(Path::toString);
    }

    @JsonProperty("isExistingTable")
    public boolean getJsonSerializableIsExistingTable()
    {
        return isExistingTable;
    }
}
