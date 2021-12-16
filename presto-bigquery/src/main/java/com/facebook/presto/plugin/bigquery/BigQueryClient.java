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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.StreamSupport;

import static com.facebook.presto.plugin.bigquery.BigQueryErrorCode.BIGQUERY_QUERY_FAILED_UNKNOWN;
import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;

public class BigQueryClient
{
    private final BigQuery bigQuery;
    private final Optional<String> viewMaterializationProject;
    private final Optional<String> viewMaterializationDataset;
    private final String tablePrefix = "_pbc_";

    // presto converts the dataset and table names to lower case, while BigQuery is case sensitive
    private final ConcurrentMap<TableId, TableId> tableIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<DatasetId, DatasetId> datasetIds = new ConcurrentHashMap<>();

    BigQueryClient(BigQuery bigQuery, BigQueryConfig config)
    {
        this.bigQuery = requireNonNull(bigQuery, "bigQuery is null");
        this.viewMaterializationProject = requireNonNull(config.getViewMaterializationProject(), "viewMaterializationProject is null");
        this.viewMaterializationDataset = requireNonNull(config.getViewMaterializationDataset(), "viewMaterializationDataset is null");
    }

    public BigQueryTable createTable(ConnectorTableMetadata meta)
    {
        Map<String, Object> tableProperties = meta.getProperties();
        String rowIdColumn = getRowIdColumn(meta);

        // Get the list of column handles
        List<BigQueryColumnHandle> columns = getColumnHandles(meta);

        // Create the BigQueryTable object
        BigQueryTable table = new BigQueryTable(
                meta.getTable().getSchemaName(),
                meta.getTable().getTableName(),
                columns,
                rowIdColumn,
                BigQueryTableProperties.isExternal(tableProperties),
                BigQueryTableProperties.getScanAuthorizations(tableProperties));

        return table;
    }

    private static String getRowIdColumn(ConnectorTableMetadata meta)
    {
        Optional<String> rowIdColumn = BigQueryTableProperties.getRowId(meta.getProperties());
        return rowIdColumn.orElse(meta.getColumns().get(0).getName()).toLowerCase(Locale.ENGLISH);
    }

    private static List<BigQueryColumnHandle> getColumnHandles(ConnectorTableMetadata meta)
    {
        // The list of indexed columns
        Optional<List<String>> indexedColumns = BigQueryTableProperties.getIndexColumns(meta.getProperties());

        // And now we parse the configured columns and create handles for the metadata manager
        ImmutableList.Builder<BigQueryColumnHandle> cBuilder = ImmutableList.builder();
        for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
            ColumnMetadata cm = meta.getColumns().get(ordinal);

            boolean indexed = indexedColumns.isPresent() && indexedColumns.get().contains(cm.getName().toLowerCase(Locale.ENGLISH));

            // Create a new object
            cBuilder.add(
                    new BigQueryColumnHandle(
                            cm.getName(), BigQueryType.valueOf(cm.getType().toString()), NULLABLE, ImmutableList.of(), null));
        }

        return cBuilder.build();
    }
    public TableInfo getTable(TableId tableId)
    {
        TableId bigQueryTableId = tableIds.get(tableId);
        Table table = bigQuery.getTable(bigQueryTableId != null ? bigQueryTableId : tableId);
        if (table != null) {
            tableIds.putIfAbsent(tableId, table.getTableId());
            datasetIds.putIfAbsent(toDatasetId(tableId), toDatasetId(table.getTableId()));
        }
        return table;
    }

    private DatasetId toDatasetId(TableId tableId)
    {
        return DatasetId.of(tableId.getProject(), tableId.getDataset());
    }

    protected String getProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    protected Iterable<Dataset> listDatasets(String projectId)
    {
        final Iterator<Dataset> datasets = bigQuery.listDatasets(projectId).iterateAll().iterator();
        return () -> Iterators.transform(datasets, this::addDataSetMappingIfNeeded);
    }

    protected Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types)
    {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        DatasetId bigQueryDatasetId = datasetIds.getOrDefault(datasetId, datasetId);
        Iterable<Table> allTables = bigQuery.listTables(bigQueryDatasetId).iterateAll();
        allTables.forEach(table -> addTableMappingIfNeeded(bigQueryDatasetId, table));
        return StreamSupport.stream(allTables.spliterator(), false)
                .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    private void addTableMappingIfNeeded(DatasetId datasetID, Table table)
    {
        TableId bigQueryTableId = table.getTableId();
        TableId prestoTableId = TableId.of(datasetID.getProject(), datasetID.getDataset(), createTableName());
        tableIds.putIfAbsent(bigQueryTableId, prestoTableId);
    }
    private Dataset addDataSetMappingIfNeeded(Dataset dataset)
    {
        DatasetId bigQueryDatasetId = dataset.getDatasetId();
        DatasetId prestoDatasetId = DatasetId.of(bigQueryDatasetId.getProject(), bigQueryDatasetId.getDataset().toLowerCase(ENGLISH));
        datasetIds.putIfAbsent(prestoDatasetId, bigQueryDatasetId);
        return dataset;
    }

    protected TableId createDestinationTable(TableId tableId)
    {
        String project = viewMaterializationProject.orElse(tableId.getProject());
        String dataset = viewMaterializationDataset.orElse(tableId.getDataset());
        DatasetId datasetId = mapIfNeeded(project, dataset);
        return TableId.of(datasetId.getProject(), datasetId.getDataset(), createTableName());
    }

    private String createTableName()
    {
        return format(tablePrefix + "%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
    }

    private DatasetId mapIfNeeded(String project, String dataset)
    {
        DatasetId datasetId = DatasetId.of(project, dataset);
        return datasetIds.getOrDefault(datasetId, datasetId);
    }

    protected Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    protected Job create(JobInfo jobInfo)
    {
        return bigQuery.create(jobInfo);
    }

    protected TableResult query(String sql)
    {
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BIGQUERY_QUERY_FAILED_UNKNOWN.toErrorCode().getCode(), format("Failed to run the query [%s]", sql), e);
        }
    }

    protected String createSql(TableId table, List<String> requiredColumns)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return createFormatSql(table, columns, new String[] {});
    }

    protected String createFormatSql(TableId table, String requiredColumns, String[] filters)
    {
        String tableName = fullTableName(table);

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return format("SELECT %s FROM `%s` %s", requiredColumns, tableName, whereClause);
    }

    // return empty if no filters are used
    private static Optional<String> createWhereClause(String[] filters)
    {
        if (filters == null || filters.length == 0) {
            return Optional.empty();
        }
        return Optional.of(String.join("", filters));
    }

    private String fullTableName(TableId tableId)
    {
        tableId = tableIds.getOrDefault(tableId, tableId);
        return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }
}
