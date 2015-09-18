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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.localfile.LocalFileMetadata.PRESTO_LOGS_SCHEMA;
import static com.facebook.presto.localfile.LocalFileMetadata.SERVER_ADDRESS_COLUMN;
import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable.getSchemaTableName;
import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable.getServerAddressColumn;
import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable.getTimestampColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LocalFileTables
{
    private final Map<SchemaTableName, DataLocation> tableDataLocations;
    private final Map<SchemaTableName, LocalFileTableHandle> tables;
    private final Map<SchemaTableName, List<ColumnMetadata>> tableColumns;

    private final LoadingCache<SchemaTableName, List<File>> cachedFiles;

    @Inject
    public LocalFileTables(LocalFileConfig config)
    {
        ImmutableMap.Builder<SchemaTableName, DataLocation> dataLocationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, LocalFileTableHandle> tablesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();

        String httpRequestLogLocation = config.getHttpRequestLogLocation();
        if (httpRequestLogLocation != null) {
            Optional<String> pattern = Optional.empty();
            if (config.getHttpRequestLogFileNamePattern() != null) {
                pattern = Optional.of(config.getHttpRequestLogFileNamePattern());
            }

            SchemaTableName table = getSchemaTableName();
            DataLocation dataLocation = new DataLocation(httpRequestLogLocation, pattern);
            LocalFileTableHandle tableHandle = new LocalFileTableHandle(table, getTimestampColumn(), getServerAddressColumn());

            tablesBuilder.put(table, tableHandle);
            tableColumnsBuilder.put(table, HttpRequestLogTable.getColumns());
            dataLocationBuilder.put(table, dataLocation);
        }

        tables = tablesBuilder.build();
        tableColumns = tableColumnsBuilder.build();
        tableDataLocations = dataLocationBuilder.build();

        cachedFiles = CacheBuilder.newBuilder()
                .expireAfterWrite(10, SECONDS)
                .build(new CacheLoader<SchemaTableName, List<File>>()
                {
                    @Override
                    public List<File> load(@Nonnull SchemaTableName key)
                            throws Exception
                    {
                        DataLocation dataLocation = tableDataLocations.get(key);
                        return dataLocation.files();
                    }
                });
    }

    public LocalFileTableHandle getTable(SchemaTableName tableName)
    {
        return tables.get(tableName);
    }

    public List<SchemaTableName> getTables()
    {
        return ImmutableList.copyOf(tables.keySet());
    }

    public List<ColumnMetadata> getColumns(LocalFileTableHandle tableHandle)
    {
        checkArgument(tableColumns.containsKey(tableHandle.getSchemaTableName()), "Table %s not registered", tableHandle.getSchemaTableName());
        return tableColumns.get(tableHandle.getSchemaTableName());
    }

    public List<File> getFiles(SchemaTableName table)
    {
        return cachedFiles.getUnchecked(table);
    }

    public static class HttpRequestLogTable
    {
        private static final List<ColumnMetadata> COLUMNS = ImmutableList.of(
                SERVER_ADDRESS_COLUMN,
                new ColumnMetadata("timestamp", TIMESTAMP),
                new ColumnMetadata("client_address", VARCHAR),
                new ColumnMetadata("method", VARCHAR),
                new ColumnMetadata("request_uri", VARCHAR),
                new ColumnMetadata("user", VARCHAR),
                new ColumnMetadata("agent", VARCHAR),
                new ColumnMetadata("response_code", BIGINT),
                new ColumnMetadata("request_size", BIGINT),
                new ColumnMetadata("response_size", BIGINT),
                new ColumnMetadata("time_to_last_byte", BIGINT),
                new ColumnMetadata("trace_token", VARCHAR));

        private static final String TABLE_NAME = "http_request_log";

        public static List<ColumnMetadata> getColumns()
        {
            return COLUMNS;
        }

        public static SchemaTableName getSchemaTableName()
        {
            return new SchemaTableName(PRESTO_LOGS_SCHEMA, TABLE_NAME);
        }

        public static OptionalInt getTimestampColumn()
        {
            return OptionalInt.of(0);
        }

        public static OptionalInt getServerAddressColumn()
        {
            return OptionalInt.of(-1);
        }
    }
}
