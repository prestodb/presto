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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.spi.StandardWarningCode.MULTIPLE_TABLE_METADATA;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_TABLE = methodHandle(
            RegisterTableProcedure.class,
            "registerTable",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class,
            String.class);
    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    public static final String METADATA_FOLDER_NAME = "metadata";
    private static final String METADATA_FILE_EXTENSION = ".metadata.json";
    private static final Pattern METADATA_VERSION_PATTERN = Pattern.compile("(?<version>\\d+)-(?<uuid>[-a-fA-F0-9]*)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");
    private static final Pattern HADOOP_METADATA_VERSION_PATTERN = Pattern.compile("v(?<version>\\d+)(?<compression>\\.[a-zA-Z0-9]+)?" + Pattern.quote(METADATA_FILE_EXTENSION) + "(?<compression2>\\.[a-zA-Z0-9]+)?");

    @Inject
    public RegisterTableProcedure(
            IcebergMetadataFactory metadataFactory,
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
                "register_table",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("metadata_location", VARCHAR),
                        new Procedure.Argument("metadata_file", VARCHAR, false, null)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(ConnectorSession clientSession, String schema, String table, String metadataLocation, String metadataFile)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(clientSession, schema, table, metadataLocation, Optional.ofNullable(metadataFile));
        }
    }

    private void doRegisterTable(ConnectorSession clientSession, String schema, String table, String metadataLocation, Optional<String> metadataFile)
    {
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        if (!metadata.schemaExists(clientSession, schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        metadataLocation = stripTrailingSlash(metadataLocation);
        Path metadataDirectory = metadataLocation.endsWith(METADATA_FOLDER_NAME) ?
                new Path(metadataLocation) : new Path(metadataLocation, METADATA_FOLDER_NAME);
        Path metadataPath = metadataFile
                .map(metadataFileName -> new Path(metadataDirectory, metadataFileName))
                .orElseGet(() -> resolveLatestMetadataLocation(
                        clientSession,
                        getFileSystem(clientSession, hdfsEnvironment, schemaTableName, metadataDirectory),
                        metadataDirectory));

        metadata.registerTable(clientSession, schemaTableName, metadataPath);
    }

    public static FileSystem getFileSystem(ConnectorSession clientSession, HdfsEnvironment hdfsEnvironment, SchemaTableName schemaTableName, Path location)
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

    public static Path resolveLatestMetadataLocation(ConnectorSession clientSession, FileSystem fileSystem, Path metadataPath)
    {
        int maxVersion = -1;
        long lastModifiedTime = -1;
        Path metadataFile = null;
        boolean duplicateVersions = false;

        try {
            FileStatus[] files = fileSystem.listStatus(metadataPath, name -> name.getName().contains(METADATA_FILE_EXTENSION));
            for (FileStatus file : files) {
                int version = parseMetadataVersionFromFileName(file.getPath().getName());
                if (version > maxVersion) {
                    maxVersion = version;
                    metadataFile = file.getPath();
                    lastModifiedTime = file.getModificationTime();
                    duplicateVersions = false;
                }
                else if (version == maxVersion) {
                    duplicateVersions = true;

                    long modifiedTime = file.getModificationTime();
                    if (modifiedTime > lastModifiedTime) {
                        lastModifiedTime = modifiedTime;
                        metadataFile = file.getPath();
                    }
                }
            }
        }
        catch (IOException io) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, format("Unable to find metadata at location %s", metadataPath), io);
        }

        if (duplicateVersions) {
            clientSession.getWarningCollector().add(new PrestoWarning(MULTIPLE_TABLE_METADATA, format("Multiple metadata files of most recent version %d found at location %s. Using most recently modified version", maxVersion, metadataPath)));
        }
        if (metadataFile == null) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, format("No metadata found at location %s", metadataPath));
        }

        return metadataFile;
    }

    static int parseMetadataVersionFromFileName(String fileName)
    {
        Matcher matcher = METADATA_VERSION_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        matcher = HADOOP_METADATA_VERSION_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return parseInt(matcher.group("version"));
        }
        return -1;
    }
}
