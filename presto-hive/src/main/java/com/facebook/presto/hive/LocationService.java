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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Optional;

public interface LocationService
{
    LocationHandle forNewTable(String user, String queryId, String schemaName, String tableName);

    LocationHandle forExistingTable(String user, String queryId, Table table);

    /**
     * Target path for the specified existing partition.
     */
    Path targetPath(LocationHandle locationHandle, Partition partition, String partitionName);

    /**
     * Target path for the specified new partition (or unpartitioned table).
     */
    Path targetPath(LocationHandle locationHandle, Optional<String> partitionName);

    /**
     * Root directory of all paths that may be returned by targetPath.
     */
    Path targetPathRoot(LocationHandle locationHandle);

    /**
     * Temporary path for writing to the specified partition (or unpartitioned table).
     * <p>
     * When temporary path is not to be used, this function may return an empty Optional or
     * a path same as the one returned from targetPath. When it is empty, special cleanups
     * need to be carried out. Otherwise, it is not necessary.
     * <p>
     * A non-empty write path is required for new tables. However, it may be the same as
     * targetPath.
     */
    Optional<Path> writePath(LocationHandle locationHandle, Optional<String> partitionName);

    /**
     * Root directory of all paths that may be returned by writePath.
     */
    Optional<Path> writePathRoot(LocationHandle locationHandle);
}
