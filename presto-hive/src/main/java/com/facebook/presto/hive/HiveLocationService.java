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

import com.facebook.presto.hive.LocationHandle.TableType;
import com.facebook.presto.hive.LocationHandle.WriteMode;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveSessionProperties.isTemporaryStagingDirectoryEnabled;
import static com.facebook.presto.hive.HiveWriteUtils.createTemporaryPath;
import static com.facebook.presto.hive.HiveWriteUtils.getTableDefaultLocation;
import static com.facebook.presto.hive.HiveWriteUtils.isS3FileSystem;
import static com.facebook.presto.hive.LocationHandle.TableType.EXISTING;
import static com.facebook.presto.hive.LocationHandle.TableType.NEW;
import static com.facebook.presto.hive.LocationHandle.TableType.TEMPORARY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createDirectory;
import static com.facebook.presto.hive.metastore.MetastoreUtil.pathExists;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class HiveLocationService
        implements LocationService
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveLocationService(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public LocationHandle forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName, boolean tempPathRequired)
    {
        Path targetPath = getTableDefaultLocation(session, metastore, hdfsEnvironment, schemaName, tableName);

        HdfsContext context = new HdfsContext(session, schemaName, tableName, targetPath.toString(), true);
        // verify the target directory for the table
        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }
        return createLocationHandle(context, session, targetPath, NEW, tempPathRequired);
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, boolean tempPathRequired)
    {
        String tablePath = table.getStorage().getLocation();
        HdfsContext context = new HdfsContext(session, table.getDatabaseName(), table.getTableName(), tablePath, false);
        Path targetPath = new Path(tablePath);
        return createLocationHandle(context, session, targetPath, EXISTING, tempPathRequired);
    }

    @Override
    public LocationHandle forTemporaryTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, boolean tempPathRequired)
    {
        String schemaName = table.getDatabaseName();
        String tableName = table.getTableName();
        Path targetPath = getTableDefaultLocation(session, metastore, hdfsEnvironment, schemaName, tableName);
        HdfsContext context = new HdfsContext(session, schemaName, tableName, targetPath.toString(), false);
        return new LocationHandle(
                targetPath,
                targetPath,
                tempPathRequired ? Optional.of(createTemporaryPath(session, context, hdfsEnvironment, targetPath)) : Optional.empty(),
                TEMPORARY,
                DIRECT_TO_TARGET_NEW_DIRECTORY);
    }

    private LocationHandle createLocationHandle(HdfsContext context, ConnectorSession session, Path targetPath, TableType tableType, boolean tempPathRequired)
    {
        Optional<Path> tempPath = tempPathRequired ? Optional.of(createTemporaryPath(session, context, hdfsEnvironment, targetPath)) : Optional.empty();
        if (shouldUseTemporaryDirectory(session, context, targetPath)) {
            Path writePath = createTemporaryPath(session, context, hdfsEnvironment, targetPath);
            createDirectory(context, hdfsEnvironment, writePath);
            return new LocationHandle(targetPath, writePath, tempPath, tableType, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        if (tableType.equals(EXISTING)) {
            return new LocationHandle(targetPath, targetPath, tempPath, tableType, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        }
        return new LocationHandle(targetPath, targetPath, tempPath, tableType, DIRECT_TO_TARGET_NEW_DIRECTORY);
    }

    private boolean shouldUseTemporaryDirectory(ConnectorSession session, HdfsContext context, Path path)
    {
        return isTemporaryStagingDirectoryEnabled(session)
                // skip using temporary directory for S3
                && !isS3FileSystem(context, hdfsEnvironment, path);
    }

    @Override
    public WriteInfo getQueryWriteInfo(LocationHandle locationHandle)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getTempPath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getTableWriteInfo(LocationHandle locationHandle)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getTempPath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getPartitionWriteInfo(LocationHandle locationHandle, Optional<Partition> partition, String partitionName)
    {
        Optional<Path> tempPath = locationHandle.getTempPath().map(path -> new Path(path, randomUUID().toString().replaceAll("-", "_")));
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

            return new WriteInfo(targetPath, writePath, tempPath, writeMode);
        }
        else {
            // new partition
            return new WriteInfo(
                    new Path(locationHandle.getTargetPath(), partitionName),
                    new Path(locationHandle.getWritePath(), partitionName),
                    tempPath,
                    locationHandle.getWriteMode());
        }
    }
}
