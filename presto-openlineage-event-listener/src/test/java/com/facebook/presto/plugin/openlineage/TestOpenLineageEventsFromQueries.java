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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.InputFieldTransformations;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunFacet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.openlineage.OpenLineageListenerQueryRunner.OPENLINEAGE_NAMESPACE;
import static com.facebook.presto.plugin.openlineage.OpenLineageListenerQueryRunner.createEventListener;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.FAIL;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.START;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.fail;

/**
 * Runs real queries through a {@link DistributedQueryRunner} and checks the OpenLineage events the
 * listener emits for them, including the input and output datasets and the column lineage the
 * engine reports. Modeled on Trino's {@code TestOpenLineageEventsFromQueries}.
 * <p>
 * The tests pin down the current engine behavior, which differs from Trino in a few places:
 * the TPC-H and TPC-DS connectors report the schema of a table by its scale factor, so the input
 * dataset for {@code tpch.tiny.nation} is named {@code tpch.sf0.01.nation} while the column lineage
 * facet still refers to it as {@code tpch.tiny.nation}; a query over a view reports the view's base
 * tables as inputs rather than the view; and CREATE VIEW reports no inputs or outputs.
 */
@Test(singleThreaded = true)
public class TestOpenLineageEventsFromQueries
        extends AbstractTestQueryFramework
{
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(30);

    private static final List<SchemaField> NATION_SCHEMA = ImmutableList.of(
            field("nationkey", "bigint"),
            field("name", "varchar(25)"),
            field("regionkey", "bigint"),
            field("comment", "varchar(152)"));

    private static final Map<String, Set<String>> NATION_IDENTITY_LINEAGE = ImmutableMap.of(
            "nationkey", ImmutableSet.of(lineage(tpchTable("nation"), "nationkey", "DIRECT/IDENTITY")),
            "name", ImmutableSet.of(lineage(tpchTable("nation"), "name", "DIRECT/IDENTITY")),
            "regionkey", ImmutableSet.of(lineage(tpchTable("nation"), "regionkey", "DIRECT/IDENTITY")),
            "comment", ImmutableSet.of(lineage(tpchTable("nation"), "comment", "DIRECT/IDENTITY")));

    private final OpenLineageMemoryTransport transport = new OpenLineageMemoryTransport();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenLineageListenerQueryRunner.createQueryRunner(createEventListener(transport));
    }

    @AfterMethod(alwaysRun = true)
    public void clearEvents()
    {
        transport.clearProcessedEvents();
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        String query = "CREATE TABLE memory.default.ctas_from_table AS SELECT * FROM tpch.tiny.nation";
        String queryId = runQuery(query);

        RunEvent startEvent = awaitRunEvent(queryId, START);
        assertJob(startEvent, queryId, query);
        assertRunFacets(startEvent, queryId);
        assertThat(startEvent.getRun().getFacets().getNominalTime()).isNull();
        assertThat(startEvent.getRun().getFacets().getAdditionalProperties()).doesNotContainKey("presto_query_statistics");
        assertThat(prestoFacet(startEvent, "presto_metadata")).doesNotContainKey("query_plan");
        assertThat(startEvent.getInputs()).isNull();
        assertThat(startEvent.getOutputs()).isNull();

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);
        assertRunFacets(completedEvent, queryId);
        assertThat(completedEvent.getRun().getRunId()).isEqualTo(startEvent.getRun().getRunId());
        assertThat(completedEvent.getRun().getFacets().getNominalTime().getNominalStartTime())
                .isBeforeOrEqualTo(completedEvent.getRun().getFacets().getNominalTime().getNominalEndTime());
        assertThat(prestoFacet(completedEvent, "presto_metadata")).containsKey("query_plan");
        assertThat(prestoFacet(completedEvent, "presto_query_statistics"))
                .containsEntry("complete", "true")
                .containsKeys("cpuTime", "wallTime", "outputRows");

        assertThat(completedEvent.getInputs()).hasSize(1);
        assertDataset(completedEvent.getInputs().get(0), tpchDataset("nation"), NATION_SCHEMA);

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.ctas_from_table", NATION_SCHEMA);
        assertThat(columnLineage(output)).isEqualTo(NATION_IDENTITY_LINEAGE);
        assertThat(datasetLineage(output)).containsExactlyInAnyOrder(
                tpchDataset("nation") + ".nationkey", tpchDataset("nation") + ".name", tpchDataset("nation") + ".regionkey", tpchDataset("nation") + ".comment");
    }

    @Test
    public void testCreateView()
            throws Exception
    {
        String query = "CREATE VIEW memory.default.nation_view_ddl AS SELECT * FROM tpch.tiny.nation";
        String queryId = runQuery(query);

        assertJob(awaitRunEvent(queryId, START), queryId, query);
        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);
        assertRunFacets(completedEvent, queryId);
        // The engine reports no I/O metadata for CREATE VIEW, so the event carries no datasets
        assertThat(completedEvent.getInputs()).isEmpty();
        assertThat(completedEvent.getOutputs()).isEmpty();
    }

    @Test
    public void testCreateTableAsSelectFromView()
            throws Exception
    {
        runQuery("CREATE VIEW memory.default.nation_view AS SELECT * FROM tpch.tiny.nation");
        String query = "CREATE TABLE memory.default.ctas_from_view AS SELECT * FROM memory.default.nation_view";
        String queryId = runQuery(query);

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);

        // The view is expanded during analysis, so lineage points at its base table
        assertThat(completedEvent.getInputs()).hasSize(1);
        assertDataset(completedEvent.getInputs().get(0), tpchDataset("nation"), NATION_SCHEMA);

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.ctas_from_view", NATION_SCHEMA);
        assertThat(columnLineage(output)).isEqualTo(NATION_IDENTITY_LINEAGE);
    }

    @Test
    public void testCreateTableWithJoin()
            throws Exception
    {
        String query = "CREATE TABLE memory.default.ctas_join AS " +
                "SELECT n.name AS nation, COUNT(*) AS order_count, SUM(o.totalprice) AS total_revenue, AVG(o.totalprice) AS avg_order_value " +
                "FROM tpch.tiny.nation n " +
                "JOIN tpch.tiny.customer c ON n.nationkey = c.nationkey " +
                "JOIN tpch.tiny.orders o ON c.custkey = o.custkey " +
                "WHERE o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' " +
                "GROUP BY n.name " +
                "ORDER BY total_revenue DESC";
        String queryId = runQuery(query);

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);

        // Inputs only list the columns the query references
        assertThat(inputsByName(completedEvent)).containsOnlyKeys(tpchDataset("nation"), tpchDataset("customer"), tpchDataset("orders"));
        assertDataset(inputsByName(completedEvent).get(tpchDataset("nation")), tpchDataset("nation"), ImmutableList.of(field("nationkey", "bigint"), field("name", "varchar(25)")));
        assertDataset(inputsByName(completedEvent).get(tpchDataset("customer")), tpchDataset("customer"), ImmutableList.of(field("custkey", "bigint"), field("nationkey", "bigint")));
        assertDataset(inputsByName(completedEvent).get(tpchDataset("orders")), tpchDataset("orders"), ImmutableList.of(field("orderdate", "date"), field("totalprice", "double"), field("custkey", "bigint")));

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.ctas_join", ImmutableList.of(
                field("nation", "varchar(25)"),
                field("order_count", "bigint"),
                field("total_revenue", "double"),
                field("avg_order_value", "double")));

        // Every output column carries the join keys, the filter column, the grouping column and the sort key as indirect lineage
        Set<String> indirect = ImmutableSet.of(
                lineage(tpchTable("nation"), "nationkey", "INDIRECT/JOIN"),
                lineage(tpchTable("customer"), "nationkey", "INDIRECT/JOIN"),
                lineage(tpchTable("customer"), "custkey", "INDIRECT/JOIN"),
                lineage(tpchTable("orders"), "custkey", "INDIRECT/JOIN"),
                lineage(tpchTable("orders"), "orderdate", "INDIRECT/FILTER"),
                lineage(tpchTable("nation"), "name", "INDIRECT/GROUP_BY"),
                lineage(tpchTable("orders"), "totalprice", "INDIRECT/SORT"));
        assertThat(columnLineage(output)).isEqualTo(ImmutableMap.of(
                "nation", union(indirect, lineage(tpchTable("nation"), "name", "DIRECT/IDENTITY")),
                "order_count", indirect,
                "total_revenue", union(indirect, lineage(tpchTable("orders"), "totalprice", "DIRECT/AGGREGATION")),
                "avg_order_value", union(indirect, lineage(tpchTable("orders"), "totalprice", "DIRECT/AGGREGATION"))));
    }

    @Test
    public void testCreateTableWithCte()
            throws Exception
    {
        String query = "CREATE TABLE memory.default.ctas_cte AS " +
                "WITH monthly_sales AS (" +
                "  SELECT d.d_year, d.d_moy, s.s_store_sk, s.s_store_name, SUM(ss.ss_sales_price) AS monthly_total " +
                "  FROM tpcds.tiny.store_sales ss " +
                "  JOIN tpcds.tiny.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk " +
                "  JOIN tpcds.tiny.store s ON ss.ss_store_sk = s.s_store_sk " +
                "  WHERE d.d_year = 2001 " +
                "  GROUP BY d.d_year, d.d_moy, s.s_store_sk, s.s_store_name), " +
                "store_rankings AS (" +
                "  SELECT d_year, d_moy, s_store_sk, s_store_name, monthly_total, " +
                "    RANK() OVER (PARTITION BY d_year, d_moy ORDER BY monthly_total DESC) AS store_rank " +
                "  FROM monthly_sales) " +
                "SELECT d_year, d_moy, CAST(d_year AS VARCHAR) || '-' || LPAD(CAST(d_moy AS VARCHAR), 2, '0') AS year_month, s_store_name, monthly_total, store_rank " +
                "FROM store_rankings " +
                "WHERE store_rank <= 5 " +
                "ORDER BY d_year, d_moy, store_rank";
        String queryId = runQuery(query);

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);
        assertThat(inputsByName(completedEvent)).containsOnlyKeys(tpcdsDataset("store_sales"), tpcdsDataset("date_dim"), tpcdsDataset("store"));

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.ctas_cte", ImmutableList.of(
                field("d_year", "integer"),
                field("d_moy", "integer"),
                field("year_month", "varchar"),
                field("s_store_name", "varchar(50)"),
                field("monthly_total", "decimal(38,2)"),
                field("store_rank", "bigint")));

        // Direct lineage follows the columns through both CTEs; the window function result has no direct source
        assertThat(directColumnLineage(output)).isEqualTo(ImmutableMap.of(
                "d_year", ImmutableSet.of(lineage(tpcdsTable("date_dim"), "d_year", "DIRECT/IDENTITY")),
                "d_moy", ImmutableSet.of(lineage(tpcdsTable("date_dim"), "d_moy", "DIRECT/IDENTITY")),
                "year_month", ImmutableSet.of(lineage(tpcdsTable("date_dim"), "d_year", "DIRECT/TRANSFORMATION"), lineage(tpcdsTable("date_dim"), "d_moy", "DIRECT/TRANSFORMATION")),
                "s_store_name", ImmutableSet.of(lineage(tpcdsTable("store"), "s_store_name", "DIRECT/IDENTITY")),
                "monthly_total", ImmutableSet.of(lineage(tpcdsTable("store_sales"), "ss_sales_price", "DIRECT/AGGREGATION")),
                "store_rank", ImmutableSet.of()));
        assertThat(columnLineage(output).get("store_rank")).contains(
                lineage(tpcdsTable("date_dim"), "d_year", "INDIRECT/WINDOW"),
                lineage(tpcdsTable("date_dim"), "d_moy", "INDIRECT/WINDOW"),
                lineage(tpcdsTable("store_sales"), "ss_sales_price", "INDIRECT/WINDOW"));
    }

    @Test
    public void testCreateTableWithCorrelatedSubquery()
            throws Exception
    {
        String query = "CREATE TABLE memory.default.ctas_subquery AS " +
                "SELECT s.suppkey, s.name, s.address, s.phone, n.name AS nation_name " +
                "FROM tpch.tiny.supplier s " +
                "JOIN tpch.tiny.nation n ON s.nationkey = n.nationkey " +
                "WHERE EXISTS (" +
                "  SELECT 1 FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey " +
                "  WHERE l.suppkey = s.suppkey AND o.orderdate >= DATE '1996-01-01' AND l.quantity > 30)";
        String queryId = runQuery(query);

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);
        assertThat(inputsByName(completedEvent)).containsOnlyKeys(tpchDataset("supplier"), tpchDataset("nation"), tpchDataset("lineitem"), tpchDataset("orders"));

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.ctas_subquery", ImmutableList.of(
                field("suppkey", "bigint"),
                field("name", "varchar(25)"),
                field("address", "varchar(40)"),
                field("phone", "varchar(15)"),
                field("nation_name", "varchar(25)")));

        assertThat(directColumnLineage(output)).isEqualTo(ImmutableMap.of(
                "suppkey", ImmutableSet.of(lineage(tpchTable("supplier"), "suppkey", "DIRECT/IDENTITY")),
                "name", ImmutableSet.of(lineage(tpchTable("supplier"), "name", "DIRECT/IDENTITY")),
                "address", ImmutableSet.of(lineage(tpchTable("supplier"), "address", "DIRECT/IDENTITY")),
                "phone", ImmutableSet.of(lineage(tpchTable("supplier"), "phone", "DIRECT/IDENTITY")),
                "nation_name", ImmutableSet.of(lineage(tpchTable("nation"), "name", "DIRECT/IDENTITY"))));
        // Columns used inside the EXISTS subquery are reported as indirect lineage of every output column
        assertThat(indirectColumnLineage(output).get("name")).isEqualTo(ImmutableSet.of(
                lineage(tpchTable("supplier"), "nationkey", "INDIRECT/JOIN"),
                lineage(tpchTable("nation"), "nationkey", "INDIRECT/JOIN"),
                lineage(tpchTable("lineitem"), "orderkey", "INDIRECT/JOIN"),
                lineage(tpchTable("orders"), "orderkey", "INDIRECT/JOIN"),
                lineage(tpchTable("lineitem"), "suppkey", "INDIRECT/FILTER"),
                lineage(tpchTable("supplier"), "suppkey", "INDIRECT/FILTER"),
                lineage(tpchTable("orders"), "orderdate", "INDIRECT/FILTER"),
                lineage(tpchTable("lineitem"), "quantity", "INDIRECT/FILTER")));
    }

    @Test
    public void testCreateTableWithSetOperation()
            throws Exception
    {
        for (String setOperator : ImmutableList.of("UNION", "UNION ALL", "INTERSECT", "EXCEPT")) {
            String table = "memory.default.ctas_" + setOperator.toLowerCase().replace(' ', '_');
            String query = format("CREATE TABLE %s AS SELECT nationkey FROM tpch.tiny.nation %s SELECT nationkey FROM tpch.tiny.customer", table, setOperator);
            String queryId = runQuery(query);

            RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
            assertJob(completedEvent, queryId, query);
            assertThat(inputsByName(completedEvent)).containsOnlyKeys(tpchDataset("nation"), tpchDataset("customer"));

            assertThat(completedEvent.getOutputs()).hasSize(1);
            OutputDataset output = completedEvent.getOutputs().get(0);
            assertDataset(output, table, ImmutableList.of(field("nationkey", "bigint")));
            // Both branches of the set operation feed the output column
            assertThat(columnLineage(output)).isEqualTo(ImmutableMap.of(
                    "nationkey", ImmutableSet.of(lineage(tpchTable("nation"), "nationkey", "DIRECT/IDENTITY"), lineage(tpchTable("customer"), "nationkey", "DIRECT/IDENTITY"))));
        }
    }

    @Test
    public void testInsertInto()
            throws Exception
    {
        runQuery("CREATE TABLE memory.default.insert_target AS SELECT * FROM tpch.tiny.nation WITH NO DATA");
        String query = "INSERT INTO memory.default.insert_target SELECT * FROM tpch.tiny.nation";
        String queryId = runQuery(query);

        RunEvent completedEvent = awaitRunEvent(queryId, COMPLETE);
        assertJob(completedEvent, queryId, query);
        assertThat(prestoFacet(completedEvent, "presto_query_statistics")).containsEntry("writtenOutputRows", "25");

        assertThat(completedEvent.getInputs()).hasSize(1);
        assertDataset(completedEvent.getInputs().get(0), tpchDataset("nation"), NATION_SCHEMA);

        assertThat(completedEvent.getOutputs()).hasSize(1);
        OutputDataset output = completedEvent.getOutputs().get(0);
        assertDataset(output, "memory.default.insert_target", NATION_SCHEMA);
        assertThat(columnLineage(output)).isEqualTo(NATION_IDENTITY_LINEAGE);
    }

    @Test
    public void testFailedQuery()
            throws Exception
    {
        String query = "CREATE TABLE memory.default.ctas_failed AS SELECT * FROM tpch.tiny.missing_table";
        assertThatThrownBy(() -> runQuery(query)).hasMessageContaining("Table tpch.tiny.missing_table does not exist");

        RunEvent failEvent = awaitRunEvent(query, FAIL);
        String queryId = failEvent.getJob().getName();
        assertJob(awaitRunEvent(queryId, START), queryId, query);
        assertJob(failEvent, queryId, query);
        assertRunFacets(failEvent, queryId);
        assertThat(failEvent.getRun().getFacets().getErrorMessage().getMessage()).isEqualTo("Table tpch.tiny.missing_table does not exist");
        assertThat(failEvent.getRun().getFacets().getAdditionalProperties()).containsKey("presto_query_statistics");
        assertThat(failEvent.getInputs()).isEmpty();
        assertThat(failEvent.getOutputs()).isEmpty();
    }

    @Test
    public void testSelectIsNotReportedAsCompleted()
            throws Exception
    {
        // SELECT is not in the default include-query-types, so no completion event is emitted for it.
        // Use a following CTAS as a barrier: its COMPLETE event arrives after the SELECT's would have.
        String selectQueryId = runQuery("SELECT count(*) FROM tpch.tiny.nation");
        String ctasQueryId = runQuery("CREATE TABLE memory.default.ctas_after_select AS SELECT * FROM tpch.tiny.nation");
        awaitRunEvent(ctasQueryId, COMPLETE);

        assertThat(findRunEvent(selectQueryId, COMPLETE)).isEmpty();
        assertThat(findRunEvent(selectQueryId, FAIL)).isEmpty();
    }

    private String runQuery(String query)
    {
        return ((DistributedQueryRunner) getQueryRunner()).executeWithQueryId(getSession(), query).getQueryId().toString();
    }

    private RunEvent awaitRunEvent(String queryIdOrQuery, EventType eventType)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + EVENT_TIMEOUT.toNanos();
        while (true) {
            Optional<RunEvent> event = findRunEvent(queryIdOrQuery, eventType);
            if (event.isPresent()) {
                return event.get();
            }
            if (System.nanoTime() > deadline) {
                fail(format("No %s event for %s within %s. Received events: %s", eventType, queryIdOrQuery, EVENT_TIMEOUT, receivedEvents()));
            }
            MILLISECONDS.sleep(100);
        }
    }

    /**
     * Finds a run event by query id (the job name) or, for queries that fail before an id is returned to the client, by query text.
     */
    private Optional<RunEvent> findRunEvent(String queryIdOrQuery, EventType eventType)
    {
        return transport.getProcessedEvents().stream()
                .filter(RunEvent.class::isInstance)
                .map(RunEvent.class::cast)
                .filter(event -> event.getEventType() == eventType)
                .filter(event -> queryIdOrQuery.equals(event.getJob().getName()) || queryIdOrQuery.equals(event.getJob().getFacets().getSql().getQuery()))
                .findFirst();
    }

    private List<String> receivedEvents()
    {
        return transport.getProcessedEvents().stream()
                .map(event -> event instanceof RunEvent
                        ? ((RunEvent) event).getEventType() + " " + ((RunEvent) event).getJob().getName()
                        : event.getClass().getSimpleName())
                .collect(toImmutableList());
    }

    private static void assertJob(RunEvent event, String queryId, String query)
    {
        assertThat(event.getJob().getNamespace()).isEqualTo(OPENLINEAGE_NAMESPACE);
        assertThat(event.getJob().getName()).isEqualTo(queryId);
        assertThat(event.getJob().getFacets().getSql().getQuery()).isEqualTo(query);
        assertThat(event.getJob().getFacets().getSql().getDialect()).isEqualTo("presto");
        assertThat(event.getJob().getFacets().getJobType().getIntegration()).isEqualTo("PRESTO");
        assertThat(event.getJob().getFacets().getJobType().getJobType()).isEqualTo("QUERY");
        assertThat(event.getJob().getFacets().getJobType().getProcessingType()).isEqualTo("BATCH");
    }

    private void assertRunFacets(RunEvent event, String queryId)
    {
        assertThat(event.getRun().getRunId()).isNotNull();
        assertThat(event.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("presto");
        assertThat(event.getRun().getFacets().getProcessing_engine().getVersion()).isEqualTo("testversion");
        assertThat(prestoFacet(event, "presto_metadata"))
                .containsEntry("query_id", queryId)
                .containsKey("transaction_id");
        assertThat(prestoFacet(event, "presto_query_context"))
                .containsEntry("server_address", "127.0.0.1")
                .containsEntry("environment", "testing")
                .containsEntry("user", getSession().getUser())
                .containsEntry("source", getSession().getSource().orElseThrow(AssertionError::new))
                .containsEntry("remote_client_address", "127.0.0.1")
                .containsKey("user_agent")
                .doesNotContainKeys("principal", "client_info");
    }

    private static Map<String, Object> prestoFacet(RunEvent event, String facetName)
    {
        RunFacet facet = event.getRun().getFacets().getAdditionalProperties().get(facetName);
        assertThat(facet).as("run facet %s", facetName).isNotNull();
        return facet.getAdditionalProperties();
    }

    private static Map<String, InputDataset> inputsByName(RunEvent event)
    {
        return event.getInputs().stream().collect(ImmutableMap.toImmutableMap(Dataset::getName, input -> input));
    }

    private static void assertDataset(Dataset dataset, String name, List<SchemaField> schema)
    {
        assertThat(dataset).as("dataset %s", name).isNotNull();
        assertThat(dataset.getNamespace()).isEqualTo(OPENLINEAGE_NAMESPACE);
        assertThat(dataset.getName()).isEqualTo(name);
        String qualifiedSchema = name.substring(0, name.lastIndexOf('.'));
        assertThat(dataset.getFacets().getDataSource().getName()).isEqualTo(qualifiedSchema);
        assertThat(dataset.getFacets().getDataSource().getUri().toString()).isEqualTo(OPENLINEAGE_NAMESPACE + "/" + qualifiedSchema);
        List<SchemaField> actualSchema = dataset.getFacets().getSchema().getFields().stream()
                .map(field -> field(field.getName(), field.getType()))
                .collect(toImmutableList());
        if (dataset instanceof OutputDataset) {
            assertThat(actualSchema).containsExactlyElementsOf(schema);
        }
        else {
            // Input columns are collected into a set by the engine, so their order is not significant
            assertThat(actualSchema).containsExactlyInAnyOrder(schema.toArray(new SchemaField[0]));
        }
    }

    /**
     * Column lineage of an output dataset as {@code output column -> {"<dataset>.<column> <TYPE>/<SUBTYPE>", ...}}.
     */
    private static Map<String, Set<String>> columnLineage(OutputDataset output)
    {
        ImmutableMap.Builder<String, Set<String>> result = ImmutableMap.builder();
        output.getFacets().getColumnLineage().getFields().getAdditionalProperties().forEach((column, columnLineage) ->
                result.put(column, columnLineage.getInputFields().stream()
                        .flatMap(inputField -> describeTransformations(inputField).stream())
                        .collect(toImmutableSet())));
        return result.build();
    }

    private static Map<String, Set<String>> directColumnLineage(OutputDataset output)
    {
        return filterColumnLineage(output, "DIRECT/");
    }

    private static Map<String, Set<String>> indirectColumnLineage(OutputDataset output)
    {
        return filterColumnLineage(output, "INDIRECT/");
    }

    private static Map<String, Set<String>> filterColumnLineage(OutputDataset output, String transformationPrefix)
    {
        ImmutableMap.Builder<String, Set<String>> result = ImmutableMap.builder();
        columnLineage(output).forEach((column, lineage) ->
                result.put(column, lineage.stream()
                        .filter(entry -> entry.substring(entry.indexOf(' ') + 1).startsWith(transformationPrefix))
                        .collect(toImmutableSet())));
        return result.build();
    }

    private static List<String> describeTransformations(InputField inputField)
    {
        String source = inputField.getName() + "." + inputField.getField();
        List<InputFieldTransformations> transformations = inputField.getTransformations();
        if (transformations == null || transformations.isEmpty()) {
            return ImmutableList.of(source);
        }
        return transformations.stream()
                .map(transformation -> {
                    assertThat(inputField.getNamespace()).isEqualTo(OPENLINEAGE_NAMESPACE);
                    assertThat(transformation.getDescription()).isNotBlank();
                    assertThat(transformation.getMasking()).isFalse();
                    return source + " " + transformation.getType() + "/" + transformation.getSubtype();
                })
                .collect(toImmutableList());
    }

    /**
     * Dataset-level lineage of an output dataset as {@code "<dataset>.<column>"} entries.
     */
    private static List<String> datasetLineage(OutputDataset output)
    {
        return output.getFacets().getColumnLineage().getDataset().stream()
                .map(inputField -> inputField.getName() + "." + inputField.getField())
                .collect(toImmutableList());
    }

    /**
     * Dataset names come from the connector's table metadata; the TPC-H and TPC-DS connectors report the
     * schema of a table by its scale factor, so {@code tpch.tiny.nation} becomes {@code tpch.sf0.01.nation}.
     */
    private static String tpchDataset(String table)
    {
        return "tpch.sf0.01." + table;
    }

    private static String tpcdsDataset(String table)
    {
        return "tpcds.sf0.01." + table;
    }

    /**
     * Column lineage refers to source tables by the name used in the query, which is not the dataset name above.
     */
    private static String tpchTable(String table)
    {
        return "tpch.tiny." + table;
    }

    private static String tpcdsTable(String table)
    {
        return "tpcds.tiny." + table;
    }

    private static String lineage(String dataset, String column, String transformation)
    {
        return dataset + "." + column + " " + transformation;
    }

    private static Set<String> union(Set<String> lineage, String... more)
    {
        return ImmutableSet.<String>builder().addAll(lineage).add(more).build();
    }

    private static SchemaField field(String name, String type)
    {
        return new SchemaField(name, type);
    }

    private static final class SchemaField
    {
        private final String name;
        private final String type;

        private SchemaField(String name, String type)
        {
            this.name = name;
            this.type = type;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof SchemaField)) {
                return false;
            }
            SchemaField that = (SchemaField) other;
            return name.equals(that.name) && type.equals(that.type);
        }

        @Override
        public int hashCode()
        {
            return 31 * name.hashCode() + type.hashCode();
        }

        @Override
        public String toString()
        {
            return name + " " + type;
        }
    }
}
