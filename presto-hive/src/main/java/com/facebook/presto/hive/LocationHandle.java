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

import static java.util.Objects.requireNonNull;

public class LocationHandle
{
    private final Path targetPath;
    private final Path writePath;
    private final boolean isExistingTable;
    private final WriteMode writeMode;

    public LocationHandle(
            Path targetPath,
            Path writePath,
            boolean isExistingTable,
            WriteMode writeMode)
    {
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.isExistingTable = isExistingTable;
        this.writeMode = requireNonNull(writeMode, "writeMode is null");
    }

    @JsonCreator
    public LocationHandle(
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("writePath") String writePath,
            @JsonProperty("isExistingTable") boolean isExistingTable,
            @JsonProperty("writeMode") WriteMode writeMode)
    {
        this(
                new Path(requireNonNull(targetPath, "targetPath is null")),
                new Path(requireNonNull(writePath, "writePath is null")),
                isExistingTable,
                writeMode);
    }

    // This method should only be called by LocationService
    Path getTargetPath()
    {
        return targetPath;
    }

    // This method should only be called by LocationService
    Path getWritePath()
    {
        return writePath;
    }

    // This method should only be called by LocationService
    public WriteMode getWriteMode()
    {
        return writeMode;
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
    public String getJsonSerializableWritePath()
    {
        return writePath.toString();
    }

    @JsonProperty("isExistingTable")
    public boolean getJsonSerializableIsExistingTable()
    {
        return isExistingTable;
    }

    @JsonProperty("writeMode")
    public WriteMode getJsonSerializableWriteMode()
    {
        return writeMode;
    }

    public enum WriteMode
    {
        STAGE_AND_MOVE_TO_TARGET_DIRECTORY, // common mode for new table or existing table (both new and existing partition)
        DIRECT_TO_TARGET_NEW_DIRECTORY, // for new table in S3
        DIRECT_TO_TARGET_EXISTING_DIRECTORY, // for existing table in S3 (both new and existing partition)

        // NOTE: Insert overwrite simulation (partition drops and partition additions in the same
        // transaction get merged and become one or more partition alterations, and get submitted to
        // metastore in close succession of each other) is not supported for S3. S3 uses the last
        // mode for insert into existing table. This is hard to support because the directory
        // containing the old data cannot be deleted until commit. Nor can the old data be moved
        // (assuming Hive HDFS directory naming convention shall not be violated). As a result,
        // subsequent insertion will have to write to directory belonging to existing partition.
        // This undermines the benefit of having insert overwrite simulation. This also makes
        // dropping of old partition at commit time hard because data added after the logical
        // "drop" time was added to the directories to be dropped.
    }
}
