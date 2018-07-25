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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.LocationHandle.WriteMode;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveWriteUtils.createTemporaryPath;
import static com.facebook.presto.hive.HiveWriteUtils.getTableDefaultLocation;
import static com.facebook.presto.hive.HiveWriteUtils.isS3FileSystem;
import static com.facebook.presto.hive.HiveWriteUtils.pathExists;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveLocationService
        implements LocationService
{
    private final HdfsEnvironment hdfsEnvironment;
    private final Map<String, String> viewFsTempDirMapping;

    @Inject
    public HiveLocationService(HdfsEnvironment hdfsEnvironment, HiveClientConfig config)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.viewFsTempDirMapping = requireNonNull(config.getViewFsTempDirMapping(), "HiveClientConfig is null");
    }

    @Override
    public LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName)
    {
        HdfsContext context = new HdfsContext(session, schemaName, tableName);
        Path targetPath = getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for the table
        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        if (shouldUseTemporaryDirectory(context, targetPath)) {
            Path writePath = createTemporaryPath(context, hdfsEnvironment, targetPath, viewFsTempDirMapping);
            return new LocationHandle(targetPath, writePath, false, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        else {
            return new LocationHandle(targetPath, targetPath, false, DIRECT_TO_TARGET_NEW_DIRECTORY);
        }
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        HdfsContext context = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
        Path targetPath = new Path(table.getStorage().getLocation());

        if (shouldUseTemporaryDirectory(context, targetPath)) {
            Path writePath = createTemporaryPath(context, hdfsEnvironment, targetPath, Collections.emptyMap());
            return new LocationHandle(targetPath, writePath, true, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        else {
            return new LocationHandle(targetPath, targetPath, true, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        }
    }

    private boolean shouldUseTemporaryDirectory(HdfsContext context, Path path)
    {
        // skip using temporary directory for S3
        return !isS3FileSystem(context, hdfsEnvironment, path);
    }

    @Override
    public WriteInfo getQueryWriteInfo(LocationHandle locationHandle)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getTableWriteInfo(LocationHandle locationHandle)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getPartitionWriteInfo(LocationHandle locationHandle, Optional<Partition> partition, String partitionName)
    {
        if (partition.isPresent()) {
            // existing partition
            WriteMode writeMode = locationHandle.getWriteMode();
            Path targetPath = new Path(partition.get().getStorage().getLocation());

            Path writePath;
            switch (writeMode) {
                case STAGE_AND_MOVE_TO_TARGET_DIRECTORY:
                    writePath = new Path(locationHandle.getWritePath(), partitionName);
                    break;
                case DIRECT_TO_TARGET_EXISTING_DIRECTORY:
                    writePath = targetPath;
                    break;
                case DIRECT_TO_TARGET_NEW_DIRECTORY:
                default:
                    throw new UnsupportedOperationException(format("inserting into existing partition is not supported for %s", writeMode));
            }

            return new WriteInfo(targetPath, writePath, writeMode);
        }
        else {
            // new partition
            return new WriteInfo(
                    new Path(locationHandle.getTargetPath(), partitionName),
                    new Path(locationHandle.getWritePath(), partitionName),
                    locationHandle.getWriteMode());
        }
    }
}
