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

package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestOutputColumnTypes
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(10);

    private QueryRunner queryRunner;
    private final CatalogType catalogType;
    private final EventsBuilder generatedEvents = new EventsBuilder();
    private final Session session;

    public TestOutputColumnTypes()
            throws Exception
    {
        this.catalogType = CatalogType.HIVE;
        this.queryRunner = createQueryRunner();
        this.queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected)
            throws Exception
    {
        return runQueryAndWaitForEvents(sql, numEventsExpected, session);
    }

    private MaterializedResult runQueryAndWaitForEvents(@Language("SQL") String sql, int numEventsExpected, Session session)
            throws Exception
    {
        generatedEvents.initialize(numEventsExpected);
        MaterializedResult result = queryRunner.execute(session, sql);
        generatedEvents.waitForEvents(EVENT_TIMEOUT);
        return result;
    }

    @Test
    public void testOutputColumnsForInsertAsSelect()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_insert_table1 AS SELECT clerk, orderkey, totalprice FROM orders", 2);
        runQueryAndWaitForEvents("INSERT INTO create_insert_table1 SELECT clerk, orderkey, totalprice FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_insert_table1");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("INSERT");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForInsertAsSelectAllWithAliasedRelation()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_insert_output1 AS SELECT clerk AS test_clerk, orderkey AS test_orderkey, totalprice AS test_totalprice FROM orders", 2);
        runQueryAndWaitForEvents("INSERT INTO create_insert_output1(test_clerk,test_orderkey,test_totalprice) SELECT clerk AS test_clerk, orderkey AS test_orderkey, totalprice AS test_totalprice FROM (SELECT * from orders) orders_a", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_insert_output1");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("INSERT");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("test_totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForInsertAsSelectColumnAliasInAliasedRelation()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_insert_output2 AS SELECT clerk AS test_clerk, orderkey AS test_orderkey, totalprice AS test_totalprice FROM orders", 2);
        runQueryAndWaitForEvents("INSERT INTO create_insert_output2(test_clerk,test_orderkey,test_totalprice) SELECT aliased_clerk AS test_clerk, aliased_orderkey AS test_orderkey, aliased_totalprice AS test_totalprice FROM (SELECT clerk, orderkey, totalprice from orders) orders_a(aliased_clerk, aliased_orderkey, aliased_totalprice)", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_insert_output2");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("INSERT");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("test_totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAS()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_update_table2 AS SELECT * FROM orders ", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_update_table2");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("custkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "custkey"))),
                        new OutputColumnMetadata("orderstatus", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderstatus"))),
                        new OutputColumnMetadata("totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))),
                        new OutputColumnMetadata("orderdate", "date", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderdate"))),
                        new OutputColumnMetadata("orderpriority", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderpriority"))),
                        new OutputColumnMetadata("clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("shippriority", "integer", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "shippriority"))),
                        new OutputColumnMetadata("comment", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "comment"))));
    }

    @Test
    public void testOutputColumnsForUnnest()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_update_table22 (orderkey bigint, custkey bigint, orderstatus ARRAY(varchar))", 2);
        runQueryAndWaitForEvents("INSERT INTO create_update_table22 VALUES (10, 20, ARRAY['SUCCESS', 'FAILED']), (22, 30, ARRAY['FAILED', 'PENDING'])", 2);
        runQueryAndWaitForEvents("CREATE TABLE create_update_table22_new AS  SELECT o.orderkey, o.custkey, s1.orderstatus AS status FROM create_update_table22 o CROSS JOIN UNNEST(o.orderstatus) AS s1(orderstatus)", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_update_table22_new");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "create_update_table22"), "orderkey"))),
                        new OutputColumnMetadata("custkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "create_update_table22"), "custkey"))),
                        new OutputColumnMetadata("status", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "create_update_table22"), "orderstatus"))));
    }

    @Test
    public void testOutputColumnsForMultipleUnnest()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE employee_details (emp_id int,emp_name varchar(100),skills ARRAY(varchar),certifications ARRAY(varchar),projects ARRAY(varchar))", 2);
        runQueryAndWaitForEvents("INSERT INTO employee_details VALUES(1, 'John Doe', ARRAY['SQL', 'Python', 'Kubernetes'], ARRAY['AWS Certified', 'Azure Certified'],ARRAY['Project A', 'Project B']),(2, 'Jane Smith', ARRAY['Java', 'Docker'], ARRAY['GCP Certified'],ARRAY['Project C'])", 2);
        runQueryAndWaitForEvents("CREATE TABLE employee_skills_certs AS SELECT e.emp_id,e.emp_name,t1.skill AS employee_skill,t2.cert AS employee_certification FROM employee_details e\n" +
                "CROSS JOIN UNNEST(e.skills) AS t1(skill)\n" +
                "CROSS JOIN UNNEST(e.certifications) AS t2(cert)", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("employee_skills_certs");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("emp_id", "integer", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "employee_details"), "emp_id"))),
                        new OutputColumnMetadata("emp_name", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "employee_details"), "emp_name"))),
                        new OutputColumnMetadata("employee_skill", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "employee_details"), "skills"))),
                        new OutputColumnMetadata("employee_certification", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "employee_details"), "certifications"))));
    }

    @Test
    public void testOutputColumnsForSingleUnnestWithTwoExpressions()
            throws Exception
    {
        runQueryAndWaitForEvents(
                "CREATE TABLE student_scores( student_name VARCHAR, subjects ARRAY(VARCHAR), scores ARRAY(INTEGER))", 2);
        runQueryAndWaitForEvents(
                "INSERT INTO  student_scores VALUES('Alice', ARRAY['Math', 'Science', 'English'], ARRAY[95, 88, 92]), ('Bob',   ARRAY['Math', 'Science', 'English'], ARRAY[78, 85, 90]),('Carol', ARRAY['Math', 'Science'], ARRAY[88, 91])", 2);
        runQueryAndWaitForEvents(
                "CREATE TABLE student_scores_details AS SELECT student_name, u.subject, u.score FROM  student_scores\n" +
                        "CROSS JOIN UNNEST(subjects, scores) AS u(subject, score)", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("student_name", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_scores"), "student_name"))),
                        new OutputColumnMetadata("subject", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_scores"), "subjects"))),
                        new OutputColumnMetadata("score", "integer", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_scores"), "scores"))));
    }

    @Test
    public void testOutputColumnsForSingleUnnestWithTwoMapExpressions()
            throws Exception
    {
        runQueryAndWaitForEvents(
                "CREATE TABLE student_grade_map(" +
                        "student_name VARCHAR, " +
                        "subject_scores MAP(VARCHAR, INTEGER), " +
                        "subject_grades MAP(VARCHAR, VARCHAR))", 2);
        runQueryAndWaitForEvents(
                "INSERT INTO student_grade_map VALUES" +
                        "('Alice', MAP(ARRAY['Math', 'Science', 'English'], ARRAY[95, 88, 92]), " +
                        "          MAP(ARRAY['Math', 'Science', 'English'], ARRAY['A', 'B+', 'A+']))," +
                        "('Bob',   MAP(ARRAY['Math', 'Science'], ARRAY[78, 85]), " +
                        "          MAP(ARRAY['Math', 'Science'], ARRAY['B+', 'A+']))", 2);
        runQueryAndWaitForEvents(
                "CREATE TABLE student_grade_map_details AS " +
                        "SELECT student_name, u.subject1, u.score, u.subject2, u.grade " +
                        "FROM student_grade_map " +
                        "CROSS JOIN UNNEST(subject_scores, subject_grades) AS u(subject1, score, subject2, grade)", 2);

        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("student_name", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_grade_map"), "student_name"))),
                        new OutputColumnMetadata("subject1", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_grade_map"), "subject_scores"))),
                        new OutputColumnMetadata("score", "integer", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_grade_map"), "subject_scores"))),
                        new OutputColumnMetadata("subject2", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_grade_map"), "subject_grades"))),
                        new OutputColumnMetadata("grade", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "student_grade_map"), "subject_grades"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectWithColumns()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_update_table3 AS SELECT clerk, orderkey, totalprice FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_update_table3");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectWithAlias()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE create_update_table4 AS SELECT clerk AS clerk_name, orderkey, totalprice AS annual_totalprice FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("create_update_table4");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("clerk_name", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("annual_totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectAllWithAliasedRelation()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE table_alias1 AS SELECT clerk AS test_clerk, orderkey AS test_orderkey, totalprice AS test_totalprice FROM (SELECT * from orders) orders_a", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("table_alias1");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("test_totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForCreateTableAsSelectColumnAliasInAliasedRelation()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE table_alias2 AS SELECT aliased_clerk AS test_clerk, aliased_orderkey AS test_orderkey, aliased_totalprice AS test_totalprice FROM (SELECT clerk,orderkey,totalprice from orders) orders_a(aliased_clerk,aliased_orderkey,aliased_totalprice)", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getCatalogName()).isEqualTo("iceberg");
        assertThat(event.getIoMetadata().getOutput().get().getSchema()).isEqualTo("tpch");
        assertThat(event.getIoMetadata().getOutput().get().getTable()).isEqualTo("table_alias2");
        assertThat(event.getMetadata().getUpdateQueryType().get()).isEqualTo("CREATE TABLE");

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_clerk", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"))),
                        new OutputColumnMetadata("test_totalprice", "double", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "totalprice"))));
    }

    @Test
    public void testOutputColumnsForSetOperationUnion()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE table_alias3 AS SELECT orderpriority AS test_orderpriority, orderkey AS test_orderkey FROM orders  UNION  SELECT clerk, custkey FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_orderpriority", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderpriority"),
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"),
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "custkey"))));
    }

    @Test
    public void testOutputColumnsForSetOperationUnionAll()
            throws Exception
    {
        runQueryAndWaitForEvents("CREATE TABLE table_alias4 AS SELECT orderpriority AS test_orderpriority, orderkey AS test_orderkey FROM orders  UNION ALL  SELECT clerk, custkey FROM orders", 2);
        QueryCompletedEvent event = getOnlyElement(generatedEvents.getQueryCompletedEvents());

        assertThat(event.getIoMetadata().getOutput().get().getColumns().get())
                .containsExactly(
                        new OutputColumnMetadata("test_orderpriority", "varchar", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderpriority"),
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "clerk"))),
                        new OutputColumnMetadata("test_orderkey", "bigint", ImmutableSet.of(
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "orderkey"),
                                new SourceColumn(new QualifiedObjectName("iceberg", "tpch", "orders"), "custkey"))));
    }

    static class TestingEventListenerPlugin
            implements Plugin
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListenerPlugin(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = requireNonNull(eventsBuilder, "eventsBuilder is null");
        }

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new TestingEventListenerFactory(eventsBuilder));
        }
    }

    private static class TestingEventListenerFactory
            implements EventListenerFactory
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListenerFactory(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = eventsBuilder;
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new TestingEventListener(eventsBuilder);
        }
    }

    private static class TestingEventListener
            implements EventListener
    {
        private final EventsBuilder eventsBuilder;

        public TestingEventListener(EventsBuilder eventsBuilder)
        {
            this.eventsBuilder = eventsBuilder;
        }

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            eventsBuilder.addQueryCreated(queryCreatedEvent);
        }

        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            eventsBuilder.addQueryCompleted(queryCompletedEvent);
        }

        @Override
        public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
            eventsBuilder.addSplitCompleted(splitCompletedEvent);
        }
    }

    static class EventsBuilder
    {
        private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
        private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;
        private ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents;

        private CountDownLatch eventsLatch;

        public synchronized void initialize(int numEvents)
        {
            queryCreatedEvents = ImmutableList.builder();
            queryCompletedEvents = ImmutableList.builder();
            splitCompletedEvents = ImmutableList.builder();

            eventsLatch = new CountDownLatch(numEvents);
        }
        public void waitForEvents(Duration duration)
                throws InterruptedException
        {
            eventsLatch.await(duration.toNanos(), NANOSECONDS);
        }

        public synchronized void addQueryCreated(QueryCreatedEvent event)
        {
            queryCreatedEvents.add(event);
            eventsLatch.countDown();
        }

        public synchronized void addQueryCompleted(QueryCompletedEvent event)
        {
            queryCompletedEvents.add(event);
            eventsLatch.countDown();
        }

        public synchronized void addSplitCompleted(SplitCompletedEvent event)
        {
            splitCompletedEvents.add(event);
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }
    }
}
