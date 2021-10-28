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
import com.facebook.presto.spi.PrestoException;
import com.google.cloud.BaseServiceException;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;

// A helper class, also handles view materialization
public class ReadSessionCreator
{
    private static final Logger log = Logger.get(ReadSessionCreator.class);

    private static Cache<String, TableInfo> destinationTableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(15, TimeUnit.MINUTES)
                    .maximumSize(1000)
                    .build();

    private final ReadSessionCreatorConfig config;
    private final BigQueryClient bigQueryClient;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;

    public ReadSessionCreator(
            ReadSessionCreatorConfig config,
            BigQueryClient bigQueryClient,
            BigQueryStorageClientFactory bigQueryStorageClientFactory)
    {
        this.config = config;
        this.bigQueryClient = bigQueryClient;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
    }

    public Storage.ReadSession create(TableId table, ImmutableList<String> selectedFields, Optional<String> filter, int parallelism)
    {
        TableInfo tableDetails = bigQueryClient.getTable(table);

        TableInfo actualTable = getActualTable(tableDetails, selectedFields, new String[] {});

        try (BigQueryStorageClient bigQueryStorageClient = bigQueryStorageClientFactory.createBigQueryStorageClient()) {
            ReadOptions.TableReadOptions.Builder readOptions = ReadOptions.TableReadOptions.newBuilder()
                    .addAllSelectedFields(selectedFields);
            filter.ifPresent(readOptions::setRowRestriction);

            TableReferenceProto.TableReference tableReference = toTableReference(actualTable.getTableId());

            Storage.ReadSession readSession = bigQueryStorageClient.createReadSession(
                    Storage.CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + bigQueryClient.getProjectId())
                            .setFormat(Storage.DataFormat.AVRO)
                            .setRequestedStreams(parallelism)
                            .setReadOptions(readOptions)
                            .setTableReference(tableReference)
                            // The BALANCED sharding strategy causes the server to
                            // assign roughly the same number of rows to each stream.
                            .setShardingStrategy(Storage.ShardingStrategy.BALANCED)
                            .build());

            return readSession;
        }
    }

    TableReferenceProto.TableReference toTableReference(TableId tableId)
    {
        return TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tableId.getProject())
                .setDatasetId(tableId.getDataset())
                .setTableId(tableId.getTable())
                .build();
    }

    TableInfo getActualTable(
            TableInfo table,
            ImmutableList<String> requiredColumns,
            String[] filters)
    {
        TableDefinition tableDefinition = table.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.TABLE == tableType) {
            return table;
        }
        if (TableDefinition.Type.VIEW == tableType) {
            if (!config.viewsEnabled) {
                throw new PrestoException(NOT_SUPPORTED,
                        "Views are not enabled. You can enable views by setting 'bigquery.views-enabled' to true. Notice additional cost may occur.");
            }
            // get it from the view
            String querySql = bigQueryClient.createSql(table.getTableId(), requiredColumns);
            log.debug("querySql is %s", querySql);
            try {
                return destinationTableCache.get(querySql, new DestinationTableBuilder(bigQueryClient, config, querySql, table.getTableId()));
            }
            catch (ExecutionException e) {
                throw new PrestoException(BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED, "Error creating destination table", e);
            }
        }
        else {
            // not regular table or a view
            throw new PrestoException(NOT_SUPPORTED, format("Table type '%s' of table '%s.%s' is not supported",
                    tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
        }
    }

    static class DestinationTableBuilder
            implements Callable<TableInfo>
    {
        final BigQueryClient bigQueryClient;
        final ReadSessionCreatorConfig config;
        final String querySql;
        final TableId table;

        DestinationTableBuilder(BigQueryClient bigQueryClient, ReadSessionCreatorConfig config, String querySql, TableId table)
        {
            this.bigQueryClient = bigQueryClient;
            this.config = config;
            this.querySql = querySql;
            this.table = table;
        }

        @Override
        public TableInfo call()
        {
            return createTableFromQuery();
        }

        TableInfo createTableFromQuery()
        {
            TableId destinationTable = bigQueryClient.createDestinationTable(table);
            log.debug("destinationTable is %s", destinationTable);
            JobInfo jobInfo = JobInfo.of(
                    QueryJobConfiguration
                            .newBuilder(querySql)
                            .setDestinationTable(destinationTable)
                            .build());
            log.debug("running query %s", jobInfo);
            Job job = waitForJob(bigQueryClient.create(jobInfo));
            log.debug("job has finished. %s", job);
            if (job.getStatus().getError() != null) {
                throw BigQueryUtil.convertToBigQueryException(job.getStatus().getError());
            }
            // add expiration time to the table
            TableInfo createdTable = bigQueryClient.getTable(destinationTable);
            long expirationTime = createdTable.getCreationTime() +
                    HOURS.toMillis(config.viewExpirationTimeInHours);
            Table updatedTable = bigQueryClient.update(createdTable.toBuilder()
                    .setExpirationTime(expirationTime)
                    .build());
            return updatedTable;
        }

        Job waitForJob(Job job)
        {
            try {
                return job.waitFor();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BigQueryException(BaseServiceException.UNKNOWN_CODE, format("Job %s has been interrupted", job.getJobId()), e);
            }
        }
    }
}
