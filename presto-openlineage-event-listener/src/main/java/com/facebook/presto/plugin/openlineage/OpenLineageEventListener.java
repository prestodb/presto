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
package com.facebook.presto.plugin.openlineage;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetBuilder;
import io.openlineage.client.OpenLineage.JobBuilder;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.client.OpenLineage.RunFacetsBuilder;
import io.openlineage.client.OpenLineageClient;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.openlineage.client.utils.UUIDUtils.generateStaticUUID;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class OpenLineageEventListener
        implements EventListener
{
    private static final Logger logger = Logger.get(OpenLineageEventListener.class);
    private static final ObjectMapper QUERY_STATISTICS_MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final OpenLineage openLineage;
    private final OpenLineageClient client;
    private final URI prestoURI;
    private final String jobNamespace;
    private final String datasetNamespace;
    private final Set<QueryType> includeQueryTypes;
    private final FormatInterpolator interpolator;

    public OpenLineageEventListener(OpenLineage openLineage, OpenLineageClient client, OpenLineageEventListenerConfig listenerConfig)
    {
        this.openLineage = requireNonNull(openLineage, "openLineage is null");
        this.client = requireNonNull(client, "client is null");
        requireNonNull(listenerConfig, "listenerConfig is null");
        this.prestoURI = defaultNamespace(listenerConfig.getPrestoURI());
        this.jobNamespace = listenerConfig.getNamespace().orElse(prestoURI.toString());
        this.datasetNamespace = prestoURI.toString();
        this.includeQueryTypes = ImmutableSet.copyOf(listenerConfig.getIncludeQueryTypes());
        this.interpolator = new FormatInterpolator(listenerConfig.getJobNameFormat(), OpenLineageJobInterpolatedValues.values());
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        // Presto does not have queryType on QueryCreatedEvent (only on QueryCompletedEvent),
        // so we always emit the START event — we can't filter by type until completion.
        RunEvent event = getStartEvent(queryCreatedEvent);
        client.emit(event);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (queryTypeSupported(queryCompletedEvent.getQueryType())) {
            RunEvent event = getCompletedEvent(queryCompletedEvent);
            client.emit(event);
            return;
        }
        logger.debug("Query type %s not supported. Supported query types %s",
                queryCompletedEvent.getQueryType().toString(),
                this.includeQueryTypes);
    }

    private boolean queryTypeSupported(Optional<QueryType> queryType)
    {
        return queryType
                .map(this.includeQueryTypes::contains)
                .orElse(false);
    }

    /*
     * Construct UUIDv7 from query creation time and queryId hash.
     * UUIDv7 are both globally unique and ordered.
     */
    private UUID getRunId(Instant queryCreateTime, QueryMetadata queryMetadata)
    {
        return generateStaticUUID(queryCreateTime, queryMetadata.getQueryId().getBytes(UTF_8));
    }

    private RunFacet getPrestoQueryContextFacet(QueryContext queryContext)
    {
        RunFacet queryContextFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put("server_address", queryContext.getServerAddress());
        properties.put("environment", queryContext.getEnvironment());

        // Presto has no getQueryType() on QueryContext; queryType is on the event itself.
        // We omit it from the context facet since it's not available here.

        properties.put("user", queryContext.getUser());
        // Presto has no getOriginalUser() — omitted

        queryContext.getPrincipal().ifPresent(principal ->
                properties.put("principal", principal));

        queryContext.getSource().ifPresent(source ->
                properties.put("source", source));

        queryContext.getClientInfo().ifPresent(clientInfo ->
                properties.put("client_info", clientInfo));

        queryContext.getRemoteClientAddress().ifPresent(remoteClientAddress ->
                properties.put("remote_client_address", remoteClientAddress));

        queryContext.getUserAgent().ifPresent(userAgent ->
                properties.put("user_agent", userAgent));

        // Presto has no getTraceToken() — omitted

        queryContextFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return queryContextFacet;
    }

    private RunFacet getPrestoMetadataFacet(QueryMetadata queryMetadata)
    {
        RunFacet prestoMetadataFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put("query_id", queryMetadata.getQueryId());

        queryMetadata.getPlan().ifPresent(
                queryPlan -> properties.put("query_plan", queryPlan));

        queryMetadata.getTransactionId().ifPresent(
                transactionId -> properties.put("transaction_id", transactionId));

        prestoMetadataFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return prestoMetadataFacet;
    }

    @SuppressWarnings("unchecked")
    private RunFacet getPrestoQueryStatisticsFacet(QueryStatistics queryStatistics)
    {
        RunFacet prestoQueryStatisticsFacet = openLineage.newRunFacet();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        QUERY_STATISTICS_MAPPER.convertValue(queryStatistics, HashMap.class).forEach(
                (key, value) -> {
                    if (key != null && value != null) {
                        properties.put(key.toString(), value.toString());
                    }
                });

        prestoQueryStatisticsFacet
                .getAdditionalProperties()
                .putAll(properties.buildOrThrow());

        return prestoQueryStatisticsFacet;
    }

    public RunEvent getStartEvent(QueryCreatedEvent queryCreatedEvent)
    {
        UUID runID = getRunId(queryCreatedEvent.getCreateTime(), queryCreatedEvent.getMetadata());
        RunFacetsBuilder runFacetsBuilder = getBaseRunFacetsBuilder(queryCreatedEvent.getContext());

        runFacetsBuilder.put(OpenLineagePrestoFacet.PRESTO_METADATA.asText(),
                getPrestoMetadataFacet(queryCreatedEvent.getMetadata()));
        runFacetsBuilder.put(OpenLineagePrestoFacet.PRESTO_QUERY_CONTEXT.asText(),
                getPrestoQueryContextFacet(queryCreatedEvent.getContext()));

        return openLineage.newRunEventBuilder()
                .eventType(RunEvent.EventType.START)
                .eventTime(queryCreatedEvent.getCreateTime().atZone(UTC))
                .run(openLineage.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                .job(getBaseJobBuilder(queryCreatedEvent.getContext(), queryCreatedEvent.getMetadata()).build())
                .build();
    }

    public RunEvent getCompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        UUID runID = getRunId(queryCompletedEvent.getCreateTime(), queryCompletedEvent.getMetadata());
        RunFacetsBuilder runFacetsBuilder = getBaseRunFacetsBuilder(queryCompletedEvent.getContext());

        runFacetsBuilder.put(OpenLineagePrestoFacet.PRESTO_METADATA.asText(),
                getPrestoMetadataFacet(queryCompletedEvent.getMetadata()));
        runFacetsBuilder.put(OpenLineagePrestoFacet.PRESTO_QUERY_CONTEXT.asText(),
                getPrestoQueryContextFacet(queryCompletedEvent.getContext()));
        runFacetsBuilder.put(OpenLineagePrestoFacet.PRESTO_QUERY_STATISTICS.asText(),
                getPrestoQueryStatisticsFacet(queryCompletedEvent.getStatistics()));
        runFacetsBuilder.nominalTime(
                openLineage.newNominalTimeRunFacet(
                        queryCompletedEvent.getCreateTime().atZone(ZoneOffset.UTC),
                        queryCompletedEvent.getEndTime().atZone(ZoneOffset.UTC)));

        boolean failed = queryCompletedEvent.getMetadata().getQueryState().equals("FAILED");
        if (failed) {
            queryCompletedEvent
                    .getFailureInfo()
                    .flatMap(QueryFailureInfo::getFailureMessage)
                    .ifPresent(failureMessage -> runFacetsBuilder
                            .errorMessage(openLineage
                                    .newErrorMessageRunFacetBuilder()
                                    .message(failureMessage)
                                    .build()));
        }

        return openLineage.newRunEventBuilder()
                .eventType(
                        failed
                                ? RunEvent.EventType.FAIL
                                : RunEvent.EventType.COMPLETE)
                .eventTime(queryCompletedEvent.getEndTime().atZone(UTC))
                .run(openLineage.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                .job(getBaseJobBuilder(queryCompletedEvent.getContext(), queryCompletedEvent.getMetadata()).build())
                .inputs(buildInputs(queryCompletedEvent.getIoMetadata()))
                .outputs(buildOutputs(queryCompletedEvent.getIoMetadata()))
                .build();
    }

    private RunFacetsBuilder getBaseRunFacetsBuilder(QueryContext queryContext)
    {
        return openLineage.newRunFacetsBuilder()
                .processing_engine(openLineage.newProcessingEngineRunFacetBuilder()
                        .name("presto")
                        .version(queryContext.getServerVersion())
                        .build());
    }

    private JobBuilder getBaseJobBuilder(QueryContext queryContext, QueryMetadata queryMetadata)
    {
        return openLineage.newJobBuilder()
                .namespace(this.jobNamespace)
                .name(interpolator.interpolate(new OpenLineageJobContext(queryContext, queryMetadata)))
                .facets(openLineage.newJobFacetsBuilder()
                        .jobType(openLineage.newJobTypeJobFacet("BATCH", "PRESTO", "QUERY"))
                        .sql(openLineage.newSQLJobFacet(queryMetadata.getQuery(), "presto"))
                        .build());
    }

    /**
     * Build inputs from QueryIOMetadata.
     * Unlike Trino which uses queryMetadata.getTables() (compile-time analysis),
     * Presto has no equivalent. We use ioMetadata.getInputs() (runtime) which is
     * only available in queryCompleted events.
     */
    private List<InputDataset> buildInputs(QueryIOMetadata ioMetadata)
    {
        return ioMetadata
                .getInputs()
                .stream()
                .map(input -> {
                    String datasetName = getDatasetName(input.getCatalogName(), input.getSchema(), input.getTable());
                    InputDatasetBuilder inputDatasetBuilder = openLineage
                            .newInputDatasetBuilder()
                            .namespace(this.datasetNamespace)
                            .name(datasetName);

                    DatasetFacetsBuilder datasetFacetsBuilder = openLineage.newDatasetFacetsBuilder()
                            .dataSource(openLineage.newDatasourceDatasetFacet(
                                    toQualifiedSchemaName(input.getCatalogName(), input.getSchema()),
                                    prestoURI.resolve(toQualifiedSchemaName(input.getCatalogName(), input.getSchema()))))
                            .schema(openLineage.newSchemaDatasetFacetBuilder()
                                    .fields(
                                            input.getColumnObjects()
                                                    .stream()
                                                    .map(column -> openLineage.newSchemaDatasetFacetFieldsBuilder()
                                                            .name(column.getName())
                                                            .type(column.getType())
                                                            .build())
                                                    .collect(toImmutableList()))
                                    .build());

                    return inputDatasetBuilder
                            .facets(datasetFacetsBuilder.build())
                            .build();
                })
                .collect(toImmutableList());
    }

    private List<OutputDataset> buildOutputs(QueryIOMetadata ioMetadata)
    {
        Optional<QueryOutputMetadata> outputs = ioMetadata.getOutput();
        if (outputs.isPresent()) {
            QueryOutputMetadata outputMetadata = outputs.get();
            List<OutputColumnMetadata> outputColumns = outputMetadata.getColumns().orElse(List.of());

            OpenLineage.ColumnLineageDatasetFacetFieldsBuilder columnLineageDatasetFacetFieldsBuilder = openLineage.newColumnLineageDatasetFacetFieldsBuilder();
            outputColumns.forEach(column ->
                    columnLineageDatasetFacetFieldsBuilder.put(column.getColumnName(),
                            openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                                    .inputFields(column
                                            .getSourceColumns()
                                            .stream()
                                            .map(inputColumn -> openLineage.newInputFieldBuilder()
                                                    .field(inputColumn.getColumnName())
                                                    .namespace(this.datasetNamespace)
                                                    .name(getDatasetName(
                                                            inputColumn.getTableName().getCatalogName(),
                                                            inputColumn.getTableName().getSchemaName(),
                                                            inputColumn.getTableName().getObjectName()))
                                                    .build())
                                            .collect(toImmutableList()))
                                    .build()));

            ImmutableList.Builder<OpenLineage.InputField> inputFields = ImmutableList.builder();
            ioMetadata.getInputs().forEach(input -> {
                for (com.facebook.presto.spi.eventlistener.Column column : input.getColumnObjects()) {
                    inputFields.add(openLineage.newInputFieldBuilder()
                            .field(column.getName())
                            .namespace(this.datasetNamespace)
                            .name(getDatasetName(input.getCatalogName(), input.getSchema(), input.getTable()))
                            .build());
                }
            });

            return ImmutableList.of(
                    openLineage.newOutputDatasetBuilder()
                            .namespace(this.datasetNamespace)
                            .name(getDatasetName(outputMetadata.getCatalogName(), outputMetadata.getSchema(), outputMetadata.getTable()))
                            .facets(openLineage.newDatasetFacetsBuilder()
                                    .columnLineage(openLineage.newColumnLineageDatasetFacet(columnLineageDatasetFacetFieldsBuilder.build(), inputFields.build()))
                                    .schema(openLineage.newSchemaDatasetFacetBuilder()
                                            .fields(
                                                    outputColumns.stream()
                                                            .map(column -> openLineage.newSchemaDatasetFacetFieldsBuilder()
                                                                    .name(column.getColumnName())
                                                                    .type(column.getColumnType())
                                                                    .build())
                                                            .collect(toImmutableList()))
                                            .build())
                                    .dataSource(openLineage.newDatasourceDatasetFacet(
                                            toQualifiedSchemaName(outputMetadata.getCatalogName(), outputMetadata.getSchema()),
                                            prestoURI.resolve(toQualifiedSchemaName(outputMetadata.getCatalogName(), outputMetadata.getSchema()))))
                                    .build())
                            .build());
        }
        return ImmutableList.of();
    }

    private String getDatasetName(String catalogName, String schemaName, String tableName)
    {
        return format("%s.%s.%s", catalogName, schemaName, tableName);
    }

    static URI defaultNamespace(URI uri)
    {
        if (!uri.getScheme().isEmpty()) {
            return URI.create(uri.toString().replaceFirst(uri.getScheme(), "presto"));
        }
        return URI.create("presto://" + uri);
    }

    private static String toQualifiedSchemaName(String catalogName, String schemaName)
    {
        return catalogName + "." + schemaName;
    }
}
