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
package com.facebook.presto.plugin.bigquery;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class BigQuerySplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(BigQuerySplitManager.class);

    private final BigQueryClient bigQueryClient;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final OptionalInt parallelism;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final NodeManager nodeManager;

    @Inject
    public BigQuerySplitManager(
            BigQueryConfig config,
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            NodeManager nodeManager)
    {
        requireNonNull(config, "config cannot be null");

        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient cannot be null");
        this.bigQueryStorageClientFactory = requireNonNull(bigQueryStorageClientFactory, "bigQueryStorageClientFactory cannot be null");
        this.parallelism = config.getParallelism();
        this.readSessionCreatorConfig = config.createReadSessionCreatorConfig();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager cannot be null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        BigQueryTableLayoutHandle bigQueryTableLayoutHandle = (BigQueryTableLayoutHandle) layout;
        BigQueryTableHandle bigQueryTableHandle = bigQueryTableLayoutHandle.getTable();

        TableId tableId = bigQueryTableHandle.getTableId();
        int actualParallelism = parallelism.orElse(nodeManager.getRequiredWorkerNodes().size());
        Optional<String> filter = Optional.empty();
        List<BigQuerySplit> splits = isEmptyProjectionIsRequired(bigQueryTableHandle.getProjectedColumns()) ?
                createEmptyProjection(tableId, actualParallelism, filter) :
                readFromBigQuery(tableId, bigQueryTableHandle.getProjectedColumns(), actualParallelism, filter);
        return new FixedSplitSource(splits);
    }

    private boolean isEmptyProjectionIsRequired(Optional<List<ColumnHandle>> projectedColumns)
    {
        return projectedColumns.isPresent() && projectedColumns.get().isEmpty();
    }

    private ImmutableList<BigQuerySplit> readFromBigQuery(TableId tableId, Optional<List<ColumnHandle>> projectedColumns, int actualParallelism, Optional<String> filter)
    {
        List<ColumnHandle> columns = projectedColumns.orElse(ImmutableList.of());
        ImmutableList<String> projectedColumnsNames = columns.stream()
                .map(column -> ((BigQueryColumnHandle) column).getName())
                .collect(toImmutableList());

        ReadSession readSession = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryStorageClientFactory)
                .create(tableId, projectedColumnsNames, filter, actualParallelism);

        return readSession.getStreamsList().stream()
                .map(stream -> BigQuerySplit.forStream(stream.getName(), readSession.getAvroSchema().getSchema(), columns))
                .collect(toImmutableList());
    }

    private List<BigQuerySplit> createEmptyProjection(TableId tableId, int actualParallelism, Optional<String> filter)
    {
        try {
            long numberOfRows;
            if (filter.isPresent()) {
                // count the rows based on the filter
                String sql = bigQueryClient.createFormatSql(tableId, "COUNT(*)", new String[] {filter.get()});
                TableResult result = bigQueryClient.query(sql);
                numberOfRows = result.iterateAll().iterator().next().get(0).getLongValue();
            }
            else {
                // no filters, so we can take the value from the table info
                numberOfRows = bigQueryClient.getTable(tableId).getNumRows().longValue();
            }

            long rowsPerSplit = numberOfRows / actualParallelism;
            long remainingRows = numberOfRows - (rowsPerSplit * actualParallelism); // need to be added to one of the split due to integer division
            List<BigQuerySplit> splits = range(0, actualParallelism)
                    .mapToObj(ignored -> BigQuerySplit.emptyProjection(rowsPerSplit))
                    .collect(toList());
            splits.set(0, BigQuerySplit.emptyProjection(rowsPerSplit + remainingRows));
            return splits;
        }
        catch (BigQueryException e) {
            throw new PrestoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, format("Failed to compute empty projection"), e);
        }
    }
}
