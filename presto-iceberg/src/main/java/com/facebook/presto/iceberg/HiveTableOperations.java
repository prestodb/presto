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
package com.facebook.presto.iceberg;

import com.facebook.airlift.concurrent.NotThreadSafe;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.UnknownTableTypeException;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.TABLE_COMMENT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isPrestoView;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergUtil.isIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;

@NotThreadSafe
public class HiveTableOperations
        implements TableOperations
{
    private static final Logger log = Logger.get(HiveTableOperations.class);

    public static final String METADATA_LOCATION = "metadata_location";
    public static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
    private static final String METADATA_FOLDER_NAME = "metadata";

    public static final StorageFormat STORAGE_FORMAT = StorageFormat.create(
            LazySimpleSerDe.class.getName(),
            FileInputFormat.class.getName(),
            FileOutputFormat.class.getName());

    private final ExtendedHiveMetastore metastore;
    private final MetastoreContext metastoreContext;
    private final String database;
    private final String tableName;
    private final Optional<String> owner;
    private final Optional<String> location;
    private final HdfsFileIO fileIO;

    private final IcebergHiveTableOperationsConfig config;

    private TableMetadata currentMetadata;
    private String currentMetadataLocation;
    private boolean shouldRefresh = true;
    private int version = -1;

    private static LoadingCache<String, ReentrantLock> commitLockCache;

    public HiveTableOperations(
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            IcebergHiveTableOperationsConfig config,
            ManifestFileCache manifestFileCache,
            String database,
            String table)
    {
        this(new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext),
                metastore,
                metastoreContext,
                config,
                database,
                table,
                Optional.empty(),
                Optional.empty());
    }

    public HiveTableOperations(
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            IcebergHiveTableOperationsConfig config,
            ManifestFileCache manifestFileCache,
            String database,
            String table,
            String owner,
            String location)
    {
        this(new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext),
                metastore,
                metastoreContext,
                config,
                database,
                table,
                Optional.of(requireNonNull(owner, "owner is null")),
                Optional.of(requireNonNull(location, "location is null")));
    }

    private HiveTableOperations(
            HdfsFileIO fileIO,
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            IcebergHiveTableOperationsConfig config,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.metastoreContext = requireNonNull(metastoreContext, "metastore context is null");
        this.database = requireNonNull(database, "database is null");
        this.tableName = requireNonNull(table, "table is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.location = requireNonNull(location, "location is null");
        this.config = requireNonNull(config, "config is null");
        //TODO: duration from config
        initTableLevelLockCache(TimeUnit.MINUTES.toMillis(10));
    }

    private static synchronized void initTableLevelLockCache(long evictionTimeout)
    {
        if (commitLockCache == null) {
            commitLockCache = CacheBuilder.newBuilder()
                    .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
                    .build(
                            new CacheLoader<String, ReentrantLock>()
                            {
                                @Override
                                public ReentrantLock load(String fullName)
                                {
                                    return new ReentrantLock();
                                }
                            });
        }
    }

    @Override
    public TableMetadata current()
    {
        if (shouldRefresh) {
            return refresh();
        }
        return currentMetadata;
    }

    @Override
    public TableMetadata refresh()
    {
        if (location.isPresent()) {
            refreshFromMetadataLocation(null);
            return currentMetadata;
        }

        Table table = getTable();

        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException("Not an Iceberg table: " + getSchemaTableName());
        }

        if (isPrestoView(table)) {
            throw new TableNotFoundException(new SchemaTableName(database, tableName));
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION);
        if (metadataLocation == null) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION, getSchemaTableName()));
        }

        refreshFromMetadataLocation(metadataLocation);

        return currentMetadata;
    }

    @Override
    public void commit(@Nullable TableMetadata base, TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        // if the metadata is already out of date, reject it
        if (!Objects.equals(base, current())) {
            throw new CommitFailedException("Cannot commit: stale table metadata for %s", getSchemaTableName());
        }

        // if the metadata is not changed, return early
        if (Objects.equals(base, metadata)) {
            return;
        }

        String newMetadataLocation = writeNewMetadata(metadata, version + 1);

        Table table;
        Optional<Long> lockId = Optional.empty();
        boolean useHMSLock = Optional.ofNullable(metadata.property(TableProperties.HIVE_LOCK_ENABLED, null))
                .map(Boolean::parseBoolean)
                .orElse(config.getLockingEnabled());
        ReentrantLock tableLevelMutex = commitLockCache.getUnchecked(database + "." + tableName);
        // getting a process-level lock per table to avoid concurrent commit attempts to the same table from the same
        // JVM process, which would result in unnecessary and costly HMS lock acquisition requests
        tableLevelMutex.lock();
        try {
            try {
                if (useHMSLock) {
                    lockId = metastore.lock(metastoreContext, database, tableName);
                }
                if (base == null) {
                    String tableComment = metadata.properties().get(TABLE_COMMENT);
                    Map<String, String> parameters = new HashMap<>();
                    parameters.put("EXTERNAL", "TRUE");
                    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE);
                    parameters.put(METADATA_LOCATION, newMetadataLocation);
                    if (tableComment != null) {
                        parameters.put(TABLE_COMMENT, tableComment);
                    }
                    Table.Builder builder = Table.builder()
                            .setDatabaseName(database)
                            .setTableName(tableName)
                            .setOwner(owner.orElseThrow(() -> new IllegalStateException("Owner not set")))
                            .setTableType(PrestoTableType.EXTERNAL_TABLE)
                            .setDataColumns(toHiveColumns(metadata.schema().columns()))
                            .withStorage(storage -> storage.setLocation(metadata.location()))
                            .withStorage(storage -> storage.setStorageFormat(STORAGE_FORMAT))
                            .setParameters(parameters);
                    table = builder.build();
                }
                else {
                    Table currentTable = getTable();
                    checkState(currentMetadataLocation != null, "No current metadata location for existing table");
                    String metadataLocation = currentTable.getParameters().get(METADATA_LOCATION);
                    if (!currentMetadataLocation.equals(metadataLocation)) {
                        throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s", currentMetadataLocation, metadataLocation, getSchemaTableName());
                    }
                    table = Table.builder(currentTable)
                            .setDataColumns(toHiveColumns(metadata.schema().columns()))
                            .withStorage(storage -> storage.setLocation(metadata.location()))
                            .setParameter(METADATA_LOCATION, newMetadataLocation)
                            .setParameter(PREVIOUS_METADATA_LOCATION, currentMetadataLocation)
                            .build();
                }
            }
            catch (RuntimeException e) {
                try {
                    io().deleteFile(newMetadataLocation);
                }
                catch (RuntimeException exception) {
                    e.addSuppressed(exception);
                }
                throw e;
            }

            PrestoPrincipal owner = new PrestoPrincipal(USER, table.getOwner());
            PrincipalPrivileges privileges = new PrincipalPrivileges(
                    ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                            .put(table.getOwner(), new HivePrivilegeInfo(SELECT, true, owner, owner))
                            .put(table.getOwner(), new HivePrivilegeInfo(INSERT, true, owner, owner))
                            .put(table.getOwner(), new HivePrivilegeInfo(UPDATE, true, owner, owner))
                            .put(table.getOwner(), new HivePrivilegeInfo(DELETE, true, owner, owner))
                            .build(),
                    ImmutableMultimap.of());
            if (base == null) {
                metastore.createTable(metastoreContext, table, privileges, emptyList());
            }
            else {
                PartitionStatistics tableStats = metastore.getTableStatistics(metastoreContext, database, tableName);
                metastore.persistTable(metastoreContext, database, tableName, table, privileges, () -> tableStats, useHMSLock ? ImmutableMap.of() : hmsEnvContext(base.metadataFileLocation()));
            }
            deleteRemovedMetadataFiles(base, metadata);
        }
        finally {
            shouldRefresh = true;
            try {
                lockId.ifPresent(id -> metastore.unlock(metastoreContext, id));
            }
            catch (Exception e) {
                log.error(e, "Failed to unlock: %s", lockId.orElse(null));
            }
            finally {
                tableLevelMutex.unlock();
            }
        }
    }

    @Override
    public FileIO io()
    {
        return fileIO;
    }

    @Override
    public String metadataFileLocation(String filename)
    {
        TableMetadata metadata = current();
        String location;
        if (metadata != null) {
            String writeLocation = metadata.properties().get(WRITE_METADATA_LOCATION);
            if (writeLocation != null) {
                return format("%s/%s", writeLocation, filename);
            }
            location = metadata.location();
        }
        else {
            location = this.location.orElseThrow(() -> new IllegalStateException("Location not set"));
        }
        return format("%s/%s/%s", location, METADATA_FOLDER_NAME, filename);
    }

    @Override
    public LocationProvider locationProvider()
    {
        TableMetadata metadata = current();
        return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }

    private Table getTable()
    {
        return metastore.getTable(metastoreContext, database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }

    private SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(database, tableName);
    }

    private String writeNewMetadata(TableMetadata metadata, int newVersion)
    {
        String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
        OutputFile newMetadataLocation = fileIO.newOutputFile(newTableMetadataFilePath);

        // write the new metadata
        TableMetadataParser.write(metadata, newMetadataLocation);

        return newTableMetadataFilePath;
    }

    private void refreshFromMetadataLocation(String newLocation)
    {
        // use null-safe equality check because new tables have a null metadata location
        if (Objects.equals(currentMetadataLocation, newLocation)) {
            shouldRefresh = false;
            return;
        }

        AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
        try {
            Tasks.foreach(newLocation)
                    .retry(config.getTableRefreshRetries())
                    .shouldRetryTest(this::shouldRetry)
                    .exponentialBackoff(
                            config.getTableRefreshBackoffMinSleepTime().toMillis(),
                            config.getTableRefreshBackoffMaxSleepTime().toMillis(),
                            config.getTableRefreshMaxRetryTime().toMillis(),
                            config.getTableRefreshBackoffScaleFactor())
                    .run(metadataLocation -> newMetadata.set(
                            TableMetadataParser.read(fileIO, fileIO.newCachedInputFile(metadataLocation))));
        }
        catch (RuntimeException e) {
            throw new TableNotFoundException(getSchemaTableName(), "Table metadata is missing", e);
        }

        if (newMetadata.get() == null) {
            throw new TableNotFoundException(getSchemaTableName(), "failed to retrieve table metadata from " + newLocation);
        }

        String newUUID = newMetadata.get().uuid();
        if (currentMetadata != null) {
            checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
                    "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
        }

        currentMetadata = newMetadata.get();
        currentMetadataLocation = newLocation;
        version = parseVersion(newLocation);
        shouldRefresh = false;
    }

    private boolean shouldRetry(Exception exception)
    {
        return !(exception.getCause() instanceof FileNotFoundException);
    }

    private static String newTableMetadataFilePath(TableMetadata meta, int newVersion)
    {
        String codec = meta.property(METADATA_COMPRESSION, METADATA_COMPRESSION_DEFAULT);
        return metadataFileLocation(meta, format("%05d-%s%s", newVersion, randomUUID(), getFileExtension(codec)));
    }

    private static String metadataFileLocation(TableMetadata metadata, String filename)
    {
        String location = metadata.properties().get(WRITE_METADATA_LOCATION);
        if (location != null) {
            return format("%s/%s", location, filename);
        }
        return format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }

    private static int parseVersion(String metadataLocation)
    {
        int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
        int versionEnd = metadataLocation.indexOf('-', versionStart);
        try {
            return parseInt(metadataLocation.substring(versionStart, versionEnd));
        }
        catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.warn(e, "Unable to parse version from metadata location: %s", metadataLocation);
            return -1;
        }
    }

    /**
     * Deletes metadata files that are no longer needed, except for the most recent ones
     * specified by `TableProperties.METADATA_PREVIOUS_VERSIONS_MAX`.
     *
     * @param base the base TableMetadata
     * @param metadata the current TableMetadata
     */
    private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata)
    {
        if (base == null) {
            return;
        }

        boolean deleteAfterCommit =
                metadata.propertyAsBoolean(
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

        if (deleteAfterCommit) {
            Set<MetadataLogEntry> metadataFilesToRemove =
                    Sets.newHashSet(base.previousFiles());
            // TableMetadata#addPreviousFile builds up the metadata log and uses
            // TableProperties.METADATA_PREVIOUS_VERSIONS_MAX to determine how many files should stay in
            // the log, thus we don't include metadata.previousFiles() for deletion - everything else can
            // be removed
            metadataFilesToRemove.removeAll(metadata.previousFiles());
            Tasks.foreach(metadataFilesToRemove)
                    .noRetry()
                    .suppressFailureWhenFinished()
                    .onFailure((previousMetadataFile, exc) ->
                            log.warn("Delete failed for previous metadata file: %s", previousMetadataFile, exc))
                    .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
        }
    }

    private Map<String, String> hmsEnvContext(String metadataLocation)
    {
        return ImmutableMap.of(
                org.apache.iceberg.hive.HiveTableOperations.NO_LOCK_EXPECTED_KEY,
                BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                org.apache.iceberg.hive.HiveTableOperations.NO_LOCK_EXPECTED_VALUE,
                metadataLocation);
    }

    @VisibleForTesting
    public IcebergHiveTableOperationsConfig getConfig()
    {
        return config;
    }
}
