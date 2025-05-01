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
package com.facebook.presto.iceberg.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastoreConfig;
import com.facebook.presto.spi.PrestoException;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.lang.String.format;

@ThreadSafe
public class IcebergFileHiveMetastore
        extends FileHiveMetastore
{
    private static final Logger LOG = Logger.get(IcebergFileHiveMetastore.class);

    @Inject
    public IcebergFileHiveMetastore(HdfsEnvironment hdfsEnvironment, FileHiveMetastoreConfig config)
    {
        this(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
    }

    public IcebergFileHiveMetastore(HdfsEnvironment hdfsEnvironment, String catalogDirectory, String metastoreUser)
    {
        super(hdfsEnvironment, catalogDirectory, metastoreUser);
    }

    @Override
    protected void validateExternalLocation(Path externalLocation, Path catalogDirectory)
            throws IOException
    {
        FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, externalLocation);
        if (!externalFileSystem.isDirectory(externalLocation)) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "External table location does not exist");
        }
    }

    @Override
    protected void validateReplaceTableType(Table originTable, Table newTable)
    {}

    @Override
    protected void renameTable(Path originalMetadataDirectory, Path newMetadataDirectory)
    {
        Optional<Runnable> rollbackAction = Optional.empty();
        try {
            // If the directory `.prestoPermissions` exists, copy it to the new table metadata directory
            Path originTablePermissionDir = new Path(originalMetadataDirectory, PRESTO_PERMISSIONS_DIRECTORY_NAME);
            Path newTablePermissionDir = new Path(newMetadataDirectory, PRESTO_PERMISSIONS_DIRECTORY_NAME);
            if (metadataFileSystem.exists(originTablePermissionDir)) {
                if (!FileUtil.copy(metadataFileSystem, originTablePermissionDir,
                        metadataFileSystem, newTablePermissionDir, false, metadataFileSystem.getConf())) {
                    throw new IOException(format("Could not rename table. Failed to copy directory: %s to %s", originTablePermissionDir, newTablePermissionDir));
                }
                else {
                    rollbackAction = Optional.of(() -> {
                        try {
                            metadataFileSystem.delete(newTablePermissionDir, true);
                        }
                        catch (IOException e) {
                            // Ignore the exception and print a warn level log
                            LOG.warn("Could not delete table permission directory: %s", newTablePermissionDir);
                        }
                    });
                }
            }

            // Rename file `.prestoSchema` to change it to the new metadata path
            // This will atomically execute the table renaming behavior
            Path originMetadataFile = new Path(originalMetadataDirectory, PRESTO_SCHEMA_FILE_NAME);
            Path newMetadataFile = new Path(newMetadataDirectory, PRESTO_SCHEMA_FILE_NAME);
            renamePath(originMetadataFile, newMetadataFile,
                    format("Could not rename table. Failed to rename file %s to %s", originMetadataFile, newMetadataFile));

            // Subsequent action, delete the redundant directory `.prestoPermissions` from the original table metadata path
            try {
                metadataFileSystem.delete(new Path(originalMetadataDirectory, PRESTO_PERMISSIONS_DIRECTORY_NAME), true);
            }
            catch (IOException e) {
                // Ignore the exception and print a warn level log
                LOG.warn("Could not delete table permission directory: %s", originalMetadataDirectory);
            }
        }
        catch (IOException e) {
            // If table renaming fails and rollback action has already been recorded, perform the rollback action to clean up junk files
            rollbackAction.ifPresent(Runnable::run);
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
