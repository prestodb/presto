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
package com.facebook.presto.iceberg.procedure;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_UNKNOWN_MANIFEST_TYPE;
import static com.facebook.presto.iceberg.IcebergUtil.dataLocation;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.metadataLocation;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;

public class RemoveOrphanFiles
        implements Provider<Procedure>
{
    private static final int REMOVE_UNUSED_FILES_OLDER_THAN_IN_DAYS = 3;
    private static final int BATCH_DELETE_FILES_COUNT = 100;
    private static final Logger LOGGER = Logger.get(RemoveOrphanFiles.class);
    private static final MethodHandle DELETE_ORPHAN_FILES = methodHandle(
            RemoveOrphanFiles.class,
            "removeOrphanFiles",
            ConnectorSession.class,
            String.class,
            String.class,
            SqlTimestamp.class);
    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public RemoveOrphanFiles(IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "remove_orphan_files",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("older_than", TIMESTAMP, false, null)),
                DELETE_ORPHAN_FILES.bindTo(this));
    }

    public void removeOrphanFiles(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRemoveOrphanFiles(clientSession, schema, tableName, olderThan);
        }
    }

    private void doRemoveOrphanFiles(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan)
    {
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
        SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
        Table icebergTable = getIcebergTable(metadata, clientSession, schemaTableName);

        Set<String> processedManifestFilePaths = new HashSet<>();
        ImmutableSet.Builder<String> validMetadataFileNames = ImmutableSet.builder();
        ImmutableSet.Builder<String> validDataFileNames = ImmutableSet.builder();

        for (Snapshot snapshot : icebergTable.snapshots()) {
            if (snapshot.manifestListLocation() != null) {
                validMetadataFileNames.add(extractFileName(snapshot.manifestListLocation()));
            }

            for (ManifestFile manifest : snapshot.allManifests(icebergTable.io())) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validMetadataFileNames.add(extractFileName(manifest.path()));
                try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(icebergTable, manifest)) {
                    for (ContentFile<?> contentFile : manifestReader) {
                        validDataFileNames.add(extractFileName(contentFile.path().toString()));
                    }
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                }
            }
        }

        metadataFileLocations(icebergTable, false).stream()
                .map(RemoveOrphanFiles::extractFileName)
                .forEach(validMetadataFileNames::add);

        statisticsFilesLocations(icebergTable).stream()
                .map(RemoveOrphanFiles::extractFileName)
                .forEach(validMetadataFileNames::add);

        // Always reserve `version-hint.text` as it's a shortcut to find the newest version
        validMetadataFileNames.add("version-hint.text");

        // Remove unused metadata and data files older than 3 days by default
        // This default value is consistent with Spark procedure `remove_orphan_files` on Iceberg, see:
        //  https://iceberg.apache.org/docs/1.5.2/spark-procedures/#remove_orphan_files
        long expiration = olderThan == null ?
                System.currentTimeMillis() - DAYS.toMillis(REMOVE_UNUSED_FILES_OLDER_THAN_IN_DAYS) :
                olderThan.isLegacyTimestamp() ? olderThan.getMillisUtc() : olderThan.getMillis();
        scanAndDeleteInvalidFiles(icebergTable, clientSession, schemaTableName, expiration, validDataFileNames.build(), dataLocation(icebergTable));
        scanAndDeleteInvalidFiles(icebergTable, clientSession, schemaTableName, expiration, validMetadataFileNames.build(), metadataLocation(icebergTable));
    }

    private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expiration, Set<String> validFiles, String folderFullPath)
    {
        try {
            List<String> filesToDelete = new ArrayList<>();
            FileSystem fileSystem = getFileSystem(session, this.hdfsEnvironment, schemaTableName, new Path(table.location()));
            Path fullPath = new Path(folderFullPath);
            if (!fileSystem.exists(fullPath)) {
                return;
            }
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(fullPath, true);
            while (allFiles.hasNext()) {
                LocatedFileStatus entry = allFiles.next();
                if (entry.getModificationTime() <= expiration && !validFiles.contains(entry.getPath().getName())) {
                    filesToDelete.add(entry.getPath().toString());
                    if (filesToDelete.size() >= BATCH_DELETE_FILES_COUNT) {
                        LOGGER.debug("Deleting files while removing orphan files for table %s : %s", schemaTableName, filesToDelete);
                        CatalogUtil.deleteFiles(table.io(), filesToDelete, folderFullPath, true);
                        filesToDelete.clear();
                    }
                }
                else {
                    LOGGER.debug("%s retained while removing orphan files %s", entry.getPath().toString(), schemaTableName.getTableName());
                }
            }

            if (!filesToDelete.isEmpty()) {
                LOGGER.debug("Deleting files while removing orphan files for table %s : %s", schemaTableName, filesToDelete);
                CatalogUtil.deleteFiles(table.io(), filesToDelete, folderFullPath, true);
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    private static String extractFileName(String path)
    {
        return path.substring(path.lastIndexOf(SEPARATOR) + 1);
    }

    private static FileSystem getFileSystem(ConnectorSession clientSession, HdfsEnvironment hdfsEnvironment, SchemaTableName schemaTableName, Path location)
    {
        HdfsContext hdfsContext = new HdfsContext(
                clientSession,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                location.getName(),
                true);

        try {
            return hdfsEnvironment.getFileSystem(hdfsContext, location);
        }
        catch (Exception e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, format("Error getting file system at path %s", location), e);
        }
    }

    private static ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest)
    {
        switch (manifest.content()) {
            case DATA:
                return ManifestFiles.read(manifest, table.io());
            case DELETES:
                return ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
            default:
                throw new PrestoException(ICEBERG_UNKNOWN_MANIFEST_TYPE, "Unknown manifest file content: " + manifest.content());
        }
    }
}
