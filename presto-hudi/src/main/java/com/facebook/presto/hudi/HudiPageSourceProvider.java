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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hudi.HudiParquetPageSources.createParquetPageSource;
import static com.facebook.presto.hudi.HudiSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hudi.HudiSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hudi.HudiSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final TypeManager typeManager;

    @Inject
    public HudiPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            TypeManager typeManager)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layoutHandle,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        HudiTableLayoutHandle layout = (HudiTableLayoutHandle) layoutHandle;
        HudiSplit hudiSplit = (HudiSplit) split;
        HudiFile baseFile = hudiSplit.getBaseFile().orElseThrow(() ->
                new PrestoException(HUDI_CANNOT_OPEN_SPLIT, "Split without base file is invalid"));
        Path path = new Path(baseFile.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(
                new HdfsContext(session,
                        layout.getTable().getSchemaName(),
                        layout.getTable().getTableName(),
                        baseFile.getPath(),
                        false),
                path);
        HudiTableType tableType = layout.getTable().getTableType();
        List<HudiColumnHandle> hudiColumnHandles = columns.stream().map(HudiColumnHandle.class::cast).collect(toList());
        List<HudiColumnHandle> dataColumns = hudiColumnHandles.stream().filter(HudiColumnHandle::isRegularColumn).collect(toList());

        final ConnectorPageSource dataColumnPageSource;
        if (tableType == HudiTableType.COW) {
            dataColumnPageSource = createParquetPageSource(
                    typeManager,
                    hdfsEnvironment,
                    session.getUser(),
                    configuration,
                    path,
                    baseFile.getStart(),
                    baseFile.getLength(),
                    dataColumns,
                    getParquetMaxReadBlockSize(session),
                    isParquetBatchReadsEnabled(session),
                    isParquetBatchReaderVerificationEnabled(session),
                    TupleDomain.all(), // TODO: predicates
                    fileFormatDataSourceStats,
                    false);
        }
        else if (tableType == HudiTableType.MOR) {
            Properties schema = getHiveSchema(
                    hudiSplit.getPartition().getStorage(),
                    toMetastoreColumns(hudiSplit.getPartition().getDataColumns()),
                    toMetastoreColumns(layout.getDataColumns()),
                    layout.getTableParameters(),
                    layout.getTable().getSchemaName(),
                    layout.getTable().getTableName(),
                    layout.getPartitionColumns().stream().map(HudiColumnHandle::getName).collect(toImmutableList()),
                    layout.getPartitionColumns().stream().map(HudiColumnHandle::getHiveType).collect(toImmutableList()));
            RecordCursor recordCursor = HudiRecordCursors.createRealtimeRecordCursor(
                    hdfsEnvironment,
                    session,
                    schema,
                    hudiSplit,
                    dataColumns,
                    DateTimeZone.UTC, // TODO configurable
                    typeManager);
            List<Type> types = dataColumns.stream()
                    .map(column -> column.getHiveType().getType(typeManager))
                    .collect(toImmutableList());
            dataColumnPageSource = new RecordPageSource(types, recordCursor);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Could not create page source for table type " + tableType);
        }

        return new HudiPageSource(
                hudiColumnHandles,
                hudiSplit.getPartition().getKeyValues(),
                dataColumnPageSource,
                session.getSqlFunctionProperties().getTimeZoneKey(),
                typeManager);
    }

    private static List<Column> toMetastoreColumns(List<HudiColumnHandle> hudiColumnHandles)
    {
        return hudiColumnHandles.stream()
                .map(column -> new Column(column.getName(), column.getHiveType(), Optional.empty(), Optional.empty()))
                .collect(toImmutableList());
    }
}
