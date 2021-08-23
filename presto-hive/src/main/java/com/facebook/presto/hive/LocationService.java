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

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface LocationService
{
    LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName, boolean tempPathRequired);

    LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, boolean tempPathRequired);

    LocationHandle forTemporaryTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, boolean tempPathRequired);

    /**
     * targetPath and writePath will be root directory of all partition and table paths
     * that may be returned by {@link #getTableWriteInfo(LocationHandle)} and {@link #getPartitionWriteInfo(LocationHandle, Optional, String)} method.
     */
    WriteInfo getQueryWriteInfo(LocationHandle locationHandle);

    WriteInfo getTableWriteInfo(LocationHandle locationHandle);

    /**
     * If {@code partition} is present, returns {@code WriteInfo} for appending existing partition;
     * otherwise, returns {@code WriteInfo} for writing new partition or overwriting existing partition.
     */
    WriteInfo getPartitionWriteInfo(LocationHandle locationHandle, Optional<Partition> partition, String partitionName);

    class WriteInfo
    {
        private final Path targetPath;
        private final Path writePath;
        private final Optional<Path> tempPath;
        private final LocationHandle.WriteMode writeMode;

        public WriteInfo(Path targetPath, Path writePath, Optional<Path> tempPath, LocationHandle.WriteMode writeMode)
        {
            this.targetPath = requireNonNull(targetPath, "targetPath is null");
            this.writePath = requireNonNull(writePath, "writePath is null");
            this.tempPath = requireNonNull(tempPath, "tempPath is null");
            this.writeMode = requireNonNull(writeMode, "writeMode is null");
        }

        /**
         * Target path for the partition, unpartitioned table, or the query.
         */
        public Path getTargetPath()
        {
            return targetPath;
        }

        /**
         * Temporary path for writing to the partition, unpartitioned table or the query.
         * <p>
         * It may be the same as {@code targetPath}.
         */
        public Path getWritePath()
        {
            return writePath;
        }

        /**
         * Temporary path for temp files generated during query processing, for example, sorted table writes.
         */
        public Optional<Path> getTempPath()
        {
            return tempPath;
        }

        public LocationHandle.WriteMode getWriteMode()
        {
            return writeMode;
        }
    }
}
