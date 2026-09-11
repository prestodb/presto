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
package com.facebook.presto.hive.functions;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.functions.schema.ParquetSchemaExtractor;
import com.facebook.presto.hive.functions.schema.SchemaMerger;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.table.GenericTableReturnTypeSpecification.GENERIC_TABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReadFilesFunction
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "read_files";
    public static final String LOCATION_ARGUMENT = "LOCATION";
    public static final String FORMAT_ARGUMENT = "FORMAT";
    public static final String MAX_FILE_COUNT_ARGUMENT = "MAX_FILE_COUNT";
    public static final long DEFAULT_MAX_FILE_COUNT = 100L;

    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final TypeManager typeManager;

    @Inject
    public ReadFilesFunction(HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, TypeManager typeManager)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ReadFilesFunctionImpl();
    }

    private final class ReadFilesFunctionImpl
            extends AbstractConnectorTableFunction
    {
        ReadFilesFunctionImpl()
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name(LOCATION_ARGUMENT)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(FORMAT_ARGUMENT)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(MAX_FILE_COUNT_ARGUMENT)
                                    .type(INTEGER)
                                    .defaultValue(DEFAULT_MAX_FILE_COUNT)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            String location = requiredString(arguments, LOCATION_ARGUMENT);
            String formatString = requiredString(arguments, FORMAT_ARGUMENT);
            HiveStorageFormat format = parseFormat(formatString);
            int maxFileCount = ((Number) requireNonNullValue(arguments, MAX_FILE_COUNT_ARGUMENT)).intValue();
            if (maxFileCount <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s must be positive, got %d", MAX_FILE_COUNT_ARGUMENT, maxFileCount));
            }

            List<ReadFilesHandle.FileEntry> files = listFiles(session, location, maxFileCount);
            if (files.isEmpty()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("No files found at location: %s", location));
            }

            ParquetSchemaExtractor extractor = new ParquetSchemaExtractor(typeManager, stats);
            List<List<ColumnMetadata>> perFileSchemas = new ArrayList<>(files.size());
            List<String> perFilePaths = new ArrayList<>(files.size());
            for (ReadFilesHandle.FileEntry file : files) {
                try {
                    perFileSchemas.add(extractor.extract(new Path(file.getPath()), hdfsEnvironment, session));
                    perFilePaths.add(file.getPath());
                }
                catch (IOException e) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Failed to read Parquet footer at %s: %s", file.getPath(), e.getMessage()), e);
                }
            }

            List<ColumnMetadata> mergedColumns = SchemaMerger.merge(perFileSchemas, perFilePaths, typeManager);

            ImmutableList.Builder<Descriptor.Field> descriptorFields = ImmutableList.builder();
            for (ColumnMetadata column : mergedColumns) {
                descriptorFields.add(new Descriptor.Field(Optional.of(column.getName()), Optional.of(column.getType())));
            }

            return TableFunctionAnalysis.builder()
                    .returnedType(new Descriptor(descriptorFields.build()))
                    .handle(new ReadFilesHandle(location, format, files, mergedColumns))
                    .build();
        }
    }

    private List<ReadFilesHandle.FileEntry> listFiles(ConnectorSession session, String location, int maxFileCount)
    {
        Path basePath = new Path(location);
        HdfsContext context = new HdfsContext(session);
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(context, basePath);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(basePath, true);
            List<ReadFilesHandle.FileEntry> files = new ArrayList<>();
            String basePrefix = basePath.toString();
            while (iterator.hasNext() && files.size() < maxFileCount) {
                LocatedFileStatus status = iterator.next();
                if (status.isDirectory()) {
                    continue;
                }
                if (status.getLen() == 0L) {
                    continue;
                }
                if (hasHiddenComponent(basePrefix, status.getPath().toString())) {
                    continue;
                }
                files.add(new ReadFilesHandle.FileEntry(status.getPath().toString(), status.getLen(), status.getModificationTime()));
            }
            return files;
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Location does not exist: %s", location));
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Failed to list files at %s: %s", location, e.getMessage()), e);
        }
    }

    private static boolean hasHiddenComponent(String basePrefix, String filePath)
    {
        if (!filePath.startsWith(basePrefix)) {
            return false;
        }
        String suffix = filePath.substring(basePrefix.length());
        for (String segment : suffix.split("/")) {
            if (!segment.isEmpty() && (segment.startsWith("_") || segment.startsWith("."))) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    static HiveStorageFormat parseFormat(String formatString)
    {
        HiveStorageFormat parsed;
        try {
            parsed = HiveStorageFormat.valueOf(formatString.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unknown format: %s. read_files Phase 1 supports only PARQUET.", formatString));
        }
        if (parsed != HiveStorageFormat.PARQUET) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Format %s is not supported by read_files Phase 1; only PARQUET is supported.", parsed));
        }
        return parsed;
    }

    private static String requiredString(Map<String, Argument> arguments, String name)
    {
        Object value = requireNonNullValue(arguments, name);
        return ((Slice) value).toStringUtf8();
    }

    private static Object requireNonNullValue(Map<String, Argument> arguments, String name)
    {
        ScalarArgument argument = (ScalarArgument) arguments.get(name);
        if (argument == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s argument is missing", name));
        }
        Object value = argument.getValue();
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s argument must not be null", name));
        }
        return value;
    }
}
