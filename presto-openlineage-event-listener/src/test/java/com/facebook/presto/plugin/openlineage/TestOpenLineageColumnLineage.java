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

import com.facebook.presto.common.ColumnLineageEntry;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.common.TransformationType;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenLineageColumnLineage
{
    private static final QualifiedObjectName ORDERS = QualifiedObjectName.valueOf("hive.tpch.orders");
    private static final QualifiedObjectName CUSTOMER = QualifiedObjectName.valueOf("hive.tpch.customer");
    private static final String EXPECTED_NAMESPACE = "presto://testhost";
    private static final String ORDERS_DATASET = "hive.tpch.orders";
    private static final String CUSTOMER_DATASET = "hive.tpch.customer";

    @Test
    public void testColumnLineageEmitsDirectAndIndirectSources()
    {
        OutputColumnMetadata totalRevenueColumn = OutputColumnMetadata.fromColumnLineage(
                "total_revenue",
                "double",
                ImmutableSet.of(
                        new ColumnLineageEntry(ORDERS, "totalprice", TransformationType.DIRECT, TransformationSubtype.AGGREGATION),
                        new ColumnLineageEntry(ORDERS, "orderstatus", TransformationType.INDIRECT, TransformationSubtype.FILTER),
                        new ColumnLineageEntry(CUSTOMER, "custkey", TransformationType.INDIRECT, TransformationSubtype.JOIN),
                        new ColumnLineageEntry(CUSTOMER, "nationkey", TransformationType.INDIRECT, TransformationSubtype.GROUP_BY)));

        RunEvent event = runListener(ImmutableList.of(totalRevenueColumn));

        List<OutputDataset> outputs = event.getOutputs();
        assertThat(outputs).hasSize(1);

        OpenLineage.ColumnLineageDatasetFacet columnLineageFacet = outputs.get(0).getFacets().getColumnLineage();
        assertThat(columnLineageFacet).isNotNull();

        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional perOutput =
                columnLineageFacet.getFields().getAdditionalProperties().get("total_revenue");
        assertThat(perOutput).isNotNull();

        List<OpenLineage.InputField> inputFields = perOutput.getInputFields();
        assertThat(inputFields).hasSize(4);

        Map<String, OpenLineage.InputField> byField = new HashMap<>();
        for (OpenLineage.InputField field : inputFields) {
            byField.put(field.getField(), field);
        }

        OpenLineage.InputField aggregated = byField.get("totalprice");
        assertThat(aggregated).isNotNull();
        assertThat(aggregated.getName()).isEqualTo("hive.tpch.orders");
        assertThat(aggregated.getTransformations()).hasSize(1);
        assertThat(aggregated.getTransformations().get(0).getType()).isEqualTo("DIRECT");
        assertThat(aggregated.getTransformations().get(0).getSubtype()).isEqualTo("AGGREGATION");
        assertThat(aggregated.getTransformations().get(0).getDescription())
                .isEqualTo("Source column aggregated in the projection");

        OpenLineage.InputField filter = byField.get("orderstatus");
        assertThat(filter).isNotNull();
        assertThat(filter.getNamespace()).isEqualTo(EXPECTED_NAMESPACE);
        assertThat(filter.getName()).isEqualTo(ORDERS_DATASET);
        assertThat(filter.getTransformations()).hasSize(1);
        assertThat(filter.getTransformations().get(0).getType()).isEqualTo("INDIRECT");
        assertThat(filter.getTransformations().get(0).getSubtype()).isEqualTo("FILTER");
        assertThat(filter.getTransformations().get(0).getDescription())
                .isEqualTo("Source column used in a WHERE or HAVING predicate");
        assertThat(filter.getTransformations().get(0).getMasking()).isFalse();

        OpenLineage.InputField join = byField.get("custkey");
        assertThat(join).isNotNull();
        assertThat(join.getNamespace()).isEqualTo(EXPECTED_NAMESPACE);
        assertThat(join.getName()).isEqualTo(CUSTOMER_DATASET);
        assertThat(join.getTransformations()).hasSize(1);
        assertThat(join.getTransformations().get(0).getType()).isEqualTo("INDIRECT");
        assertThat(join.getTransformations().get(0).getSubtype()).isEqualTo("JOIN");

        OpenLineage.InputField groupBy = byField.get("nationkey");
        assertThat(groupBy).isNotNull();
        assertThat(groupBy.getNamespace()).isEqualTo(EXPECTED_NAMESPACE);
        assertThat(groupBy.getName()).isEqualTo(CUSTOMER_DATASET);
        assertThat(groupBy.getTransformations()).hasSize(1);
        assertThat(groupBy.getTransformations().get(0).getType()).isEqualTo("INDIRECT");
        assertThat(groupBy.getTransformations().get(0).getSubtype()).isEqualTo("GROUP_BY");
    }

    @Test
    public void testIndirectAttachesToExistingDirectInputField()
    {
        OutputColumnMetadata column = OutputColumnMetadata.fromColumnLineage(
                "customer_name",
                "varchar",
                ImmutableSet.of(
                        new ColumnLineageEntry(CUSTOMER, "name", TransformationType.DIRECT, TransformationSubtype.IDENTITY),
                        new ColumnLineageEntry(CUSTOMER, "name", TransformationType.INDIRECT, TransformationSubtype.JOIN)));

        RunEvent event = runListener(ImmutableList.of(column));

        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional perOutput =
                event.getOutputs().get(0).getFacets().getColumnLineage()
                        .getFields().getAdditionalProperties().get("customer_name");

        List<OpenLineage.InputField> inputFields = perOutput.getInputFields();
        assertThat(inputFields).hasSize(1);
        OpenLineage.InputField input = inputFields.get(0);
        assertThat(input.getField()).isEqualTo("name");
        assertThat(input.getTransformations()).hasSize(2);
        assertThat(input.getTransformations())
                .extracting(transformation -> transformation.getSubtype())
                .containsExactlyInAnyOrder("IDENTITY", "JOIN");
    }

    @Test
    public void testNoLineageEmitsEmptyInputFieldList()
    {
        OutputColumnMetadata column = OutputColumnMetadata.fromColumnLineage(
                "literal_one",
                "integer",
                ImmutableSet.of());

        RunEvent event = runListener(ImmutableList.of(column));

        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional perOutput =
                event.getOutputs().get(0).getFacets().getColumnLineage()
                        .getFields().getAdditionalProperties().get("literal_one");

        assertThat(perOutput.getInputFields()).isEmpty();
    }

    @Test
    public void testDirectIdentityEmitsIdentityTransformation()
    {
        OutputColumnMetadata column = OutputColumnMetadata.fromColumnLineage(
                "order_id",
                "bigint",
                ImmutableSet.of(
                        new ColumnLineageEntry(ORDERS, "orderkey", TransformationType.DIRECT, TransformationSubtype.IDENTITY)));

        RunEvent event = runListener(ImmutableList.of(column));

        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional perOutput =
                event.getOutputs().get(0).getFacets().getColumnLineage()
                        .getFields().getAdditionalProperties().get("order_id");

        List<OpenLineage.InputField> inputFields = perOutput.getInputFields();
        assertThat(inputFields).hasSize(1);
        assertThat(inputFields.get(0).getField()).isEqualTo("orderkey");
        assertThat(inputFields.get(0).getTransformations()).hasSize(1);
        assertThat(inputFields.get(0).getTransformations().get(0).getType()).isEqualTo("DIRECT");
        assertThat(inputFields.get(0).getTransformations().get(0).getSubtype()).isEqualTo("IDENTITY");
        assertThat(inputFields.get(0).getTransformations().get(0).getDescription())
                .isEqualTo("Direct projection of the source column");
    }

    @Test
    public void testDirectTransformationEmitsTransformations()
    {
        OutputColumnMetadata column = OutputColumnMetadata.fromColumnLineage(
                "total_with_tax",
                "double",
                ImmutableSet.of(
                        new ColumnLineageEntry(ORDERS, "totalprice", TransformationType.DIRECT, TransformationSubtype.TRANSFORMATION)));

        RunEvent event = runListener(ImmutableList.of(column));

        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional perOutput =
                event.getOutputs().get(0).getFacets().getColumnLineage()
                        .getFields().getAdditionalProperties().get("total_with_tax");

        List<OpenLineage.InputField> inputFields = perOutput.getInputFields();
        assertThat(inputFields).hasSize(1);
        assertThat(inputFields.get(0).getTransformations()).hasSize(1);
        assertThat(inputFields.get(0).getTransformations().get(0).getType()).isEqualTo("DIRECT");
        assertThat(inputFields.get(0).getTransformations().get(0).getSubtype()).isEqualTo("TRANSFORMATION");
        assertThat(inputFields.get(0).getTransformations().get(0).getDescription())
                .isEqualTo("Source column transformed in the projection");
    }

    private static RunEvent runListener(List<OutputColumnMetadata> outputColumns)
    {
        OpenLineageEventListener listener = (OpenLineageEventListener) new OpenLineageEventListenerFactory().create(
                ImmutableMap.of(
                        "openlineage-event-listener.transport.type", "CONSOLE",
                        "openlineage-event-listener.presto.uri", "http://testhost"));

        QueryInputMetadata orderInput = new QueryInputMetadata(
                "hive",
                "tpch",
                "orders",
                ImmutableList.of(new Column("orderkey", "bigint"), new Column("totalprice", "double"), new Column("orderstatus", "varchar")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        QueryOutputMetadata outputMetadata = new QueryOutputMetadata(
                "hive",
                "tpch",
                "totals",
                Optional.empty(),
                Optional.empty(),
                Optional.of(outputColumns),
                Optional.empty());

        QueryIOMetadata io = new QueryIOMetadata(ImmutableList.of(orderInput), Optional.of(outputMetadata));

        return listener.getCompletedEvent(buildCompletedEvent(io));
    }

    private static QueryCompletedEvent buildCompletedEvent(QueryIOMetadata io)
    {
        QueryContext ctx = new QueryContext(
                "user",
                Optional.of("principal"),
                Optional.of("127.0.0.1"),
                Optional.of("Some-User-Agent"),
                Optional.of("Some client info"),
                new HashSet<>(),
                Optional.of("presto-cli"),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(),
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                "serverAddress",
                "serverVersion",
                "environment",
                "worker");

        QueryMetadata meta = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                "create table hive.tpch.totals as select sum(totalprice) total_revenue from hive.tpch.orders",
                "queryHash",
                Optional.empty(),
                "COMPLETED",
                URI.create("http://localhost"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                Optional.of("updateType"));

        QueryStatistics stats = new QueryStatistics(
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(0), Duration.ofSeconds(0), Duration.ofSeconds(0), Duration.ofSeconds(0),
                Duration.ofSeconds(0), Duration.ofSeconds(0), Duration.ofSeconds(0), Optional.empty(),
                Duration.ofSeconds(1), Duration.ofSeconds(0),
                0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0.0, 0.0, 0, true,
                new RuntimeStats());

        return new QueryCompletedEvent(
                meta, stats, ctx, io,
                Optional.empty(),
                Collections.emptyList(),
                Optional.of(QueryType.INSERT),
                Collections.emptyList(),
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                Instant.parse("2025-04-28T11:24:16.256207Z"),
                Instant.parse("2025-04-28T11:24:26.993340Z"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Optional.empty(),
                Collections.emptyMap(),
                Optional.empty(),
                Optional.empty());
    }
}
