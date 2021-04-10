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
package com.facebook.presto.druid.ingestion;

import com.facebook.presto.common.Page;
import com.facebook.presto.druid.DruidClient;
import com.facebook.presto.druid.DruidConfig;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DruidPageSink
        implements ConnectorPageSink
{
    public static final String TIMESTAMP_COLUMN = "__time";

    private final DruidConfig druidConfig;
    private final DruidClient druidClient;
    private final DruidIngestionTableHandle tableHandle;
    private final DruidPageWriter druidPageWriter;
    private final Path dataPath;
    private List<String> dataFileList;

    public DruidPageSink(
            DruidConfig druidConfig,
            DruidClient druidClient,
            DruidIngestionTableHandle tableHandle,
            DruidPageWriter druidPageWriter)
    {
        this.druidConfig = requireNonNull(druidConfig, "druidConfig is null");
        this.druidClient = requireNonNull(druidClient, "druidClient is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.druidPageWriter = requireNonNull(druidPageWriter, "pageWriter is null");
        this.dataPath = new Path(druidConfig.getIngestionStoragePath(), tableHandle.getTableName() + UUID.randomUUID());
        this.dataFileList = new ArrayList<String>();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        dataFileList.add(druidPageWriter.append(page, tableHandle, dataPath).toString());
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        DruidIngestTask ingestTask = new DruidIngestTask.Builder()
                .withDataSource(tableHandle.getTableName())
                .withInputSource(dataPath, dataFileList)
                .withTimestampColumn(TIMESTAMP_COLUMN)
                .withDimensions(tableHandle.getColumns().stream()
                        .filter(column -> !column.getColumnName().equals(TIMESTAMP_COLUMN))
                        .map(column -> new DruidIngestTask.DruidIngestDimension(column.getDataType().getIngestType(), column.getColumnName()))
                        .collect(Collectors.toList()))
                .withAppendToExisting(true)
                .build();
        druidClient.ingestData(ingestTask);
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
