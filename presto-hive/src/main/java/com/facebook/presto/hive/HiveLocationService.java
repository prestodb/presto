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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveWriteUtils.createTemporaryPath;
import static com.facebook.presto.hive.HiveWriteUtils.getTableDefaultLocation;
import static com.facebook.presto.hive.HiveWriteUtils.pathExists;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveLocationService
        implements LocationService
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveLocationService(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment)
    {
        this.metastore = requireNonNull(metastore);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment);
    }

    @Override
    public LocationHandle forNewTable(String queryId, String schemaName, String tableName)
    {
        Path targetPath = getTableDefaultLocation(metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for the table
        if (pathExists(hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        Path writePath;
        if (shouldUseTemporaryDirectory(targetPath)) {
            writePath = createTemporaryPath(hdfsEnvironment, targetPath);
        }
        else {
            writePath = targetPath;
        }

        return new LocationHandle(targetPath, Optional.of(writePath), false);
    }

    @Override
    public LocationHandle forExistingTable(String queryId, Table table)
    {
        Path targetPath = new Path(table.getSd().getLocation());

        Optional<Path> writePath;
        if (shouldUseTemporaryDirectory(targetPath)) {
            writePath = Optional.of(createTemporaryPath(hdfsEnvironment, targetPath));
        }
        else {
            writePath = Optional.empty();
        }

        return new LocationHandle(targetPath, writePath, true);
    }

    private boolean shouldUseTemporaryDirectory(Path path)
    {
        try {
            // skip using temporary directory for S3
            return !(hdfsEnvironment.getFileSystem(path) instanceof PrestoS3FileSystem);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    @Override
    public Path targetPath(LocationHandle locationHandle, Partition partition, String partitionName)
    {
        return new Path(partition.getSd().getLocation());
    }

    @Override
    public Path targetPath(LocationHandle locationHandle, Optional<String> partitionName)
    {
        if (!partitionName.isPresent()) {
            return locationHandle.getTargetPath();
        }
        return new Path(locationHandle.getTargetPath(), partitionName.get());
    }

    @Override
    public Path targetPathRoot(LocationHandle locationHandle)
    {
        return locationHandle.getTargetPath();
    }

    @Override
    public Optional<Path> writePath(LocationHandle locationHandle, Optional<String> partitionName)
    {
        if (!partitionName.isPresent()) {
            return locationHandle.getWritePath();
        }
        return locationHandle.getWritePath().map(path -> new Path(path, partitionName.get()));
    }

    @Override
    public Optional<Path> writePathRoot(LocationHandle locationHandle)
    {
        return locationHandle.getWritePath();
    }
}
