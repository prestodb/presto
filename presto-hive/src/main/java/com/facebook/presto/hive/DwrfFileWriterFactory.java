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

import com.facebook.hive.orc.OrcOutputFormat;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.hive.orc.OrcConf.ConfVars.HIVE_ORC_COMPRESSION;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;

public class DwrfFileWriterFactory
        implements HiveFileWriterFactory
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public DwrfFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveClientConfig hiveClientConfig)
    {
        this(hdfsEnvironment, typeManager, nodeVersion, requireNonNull(hiveClientConfig, "hiveClientConfig is null").getDateTimeZone());
    }

    public DwrfFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone hiveStorageTimeZone)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session)
    {
        if (!HiveSessionProperties.isOrcOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        if (!OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        String compressionName = configuration.get(HIVE_ORC_COMPRESSION.varname);
        CompressionKind compression;
        try {
            compression = CompressionKind.valueOf(compressionName);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Unknown DWRF compression type " + compressionName);
        }

        // existing tables may have columns in a different order
        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            OutputStream outputStream = fileSystem.create(path);

            return Optional.of(new DwrfFileWriter(
                    outputStream,
                    fileColumnNames,
                    fileColumnTypes,
                    compression,
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(HiveMetadata.PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(HiveMetadata.PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    hiveStorageTimeZone));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating DWRF file", e);
        }
    }
}
