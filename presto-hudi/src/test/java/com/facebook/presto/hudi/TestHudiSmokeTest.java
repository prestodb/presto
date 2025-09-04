package com.facebook.presto.hudi;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new ResourceHudiTablesInitializer())
                .addConnectorProperties(getAdditionalHudiProperties())
                .build();
    }

    protected ImmutableMap<String, String> getAdditionalHudiProperties()
    {
        return ImmutableMap.of();
    }

    @Test
    public void testReadNonPartitionedTable()
    {
        assertQuery(
                "SELECT id, name FROM " + HUDI_NON_PART_COW,
                "SELECT * FROM VALUES (1, 'a1'), (2, 'a2')");
    }

    @Test
    public void testReadPartitionedTables()
    {
        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_COW + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_MOR + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");
        System.out.println(getQueryRunner().execute(getSession(), "EXPLAIN ANALYZE SELECT * FROM " + HUDI_STOCK_TICKS_COW).toString());

        System.out.println("test start");
        getQueryRunner().execute(getSession(), "SET SESSION hudi.metadata_enabled=true");
        String res = getQueryRunner().execute(getSession(), "SELECT * FROM " + HUDI_STOCK_TICKS_COW).toString();
        System.out.println(res);
        assertQuery("SELECT dt, count(1) FROM " + STOCK_TICKS_MOR + " GROUP BY dt",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    @Test
    public void testReadNonExtractablePartitionPathTable()
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .build();
        String res = getQueryRunner().execute(session, "SELECT * FROM " + HUDI_NON_EXTRACTABLE_PARTITION_PATH).toString();
        System.out.println(res);
        assertQuery(session, "SELECT name FROM " + HUDI_NON_EXTRACTABLE_PARTITION_PATH + " where dt='2018-10-05'",
                "SELECT * FROM VALUES ('Alice'), ('Bob')");
        assertQuery(session, "SELECT name FROM " + HUDI_NON_EXTRACTABLE_PARTITION_PATH + " where dt='2018-10-05' and hh='10'",
                "SELECT * FROM VALUES ('Alice'), ('Bob')");
    }

    @Test
    public void testReadPartitionedCOWTableVer8()
    {
        String res = getQueryRunner().execute(getSession(), "SELECT * FROM " + HUDI_STOCK_TICKS_COW).toString();
        System.out.println(res);
        assertQuery("SELECT date FROM " + HUDI_STOCK_TICKS_COW + " GROUP BY date",
                "SELECT * FROM VALUES ('2018-08-31')");
        assertQuery("SELECT date, count(1) FROM " + HUDI_STOCK_TICKS_COW + " GROUP BY date",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    // Verifies query results on a partitioned COW table (Ver8).
    // Since the table defines only a single partition column, its partition
    // values can be correctly parsed by Hudi's PartitionValueExtractor.
    // This test asserts that grouping and aggregations on the partition column
    // return the expected results.
    @Test
    public void testReadPartitionedMORTableVer8()
    {
        getQueryRunner().execute(getSession(), "SET SESSION hudi.metadata_enabled=true");
        String res = getQueryRunner().execute(getSession(), "SELECT * FROM " + HUDI_STOCK_TICKS_COW).toString();
        System.out.println(res);
        assertQuery("SELECT date FROM " + HUDI_STOCK_TICKS_MOR + " GROUP BY date",
                "SELECT * FROM VALUES ('2018-08-31')");
        assertQuery("SELECT date, count(1) FROM " + HUDI_STOCK_TICKS_COW + " GROUP BY date",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    @Test
    public void testBaseFileOnlyReadWithProjection()
    {
        Session session = SessionBuilder.from(getSession()).build();
        MaterializedResult countResult = getQueryRunner().execute(
                session, "SELECT count(*) FROM " + HUDI_TRIPS_COW_V8);
        assertThat(countResult.getOnlyValue()).isEqualTo(40000L);
        assertThat(countResult.getStatementStats().get().getPhysicalInputBytes()).isLessThan(500000L);
        MaterializedResult groupByResult = getQueryRunner().execute(
                session, "SELECT driver, count(*) FROM " + HUDI_TRIPS_COW_V8 + " group by 1");
        assertThat(groupByResult.getMaterializedRows().size()).isEqualTo(1);
        assertThat(groupByResult.getMaterializedRows().getFirst().getFieldCount()).isEqualTo(2);
        assertThat(groupByResult.getMaterializedRows().getFirst().getField(0)).isEqualTo("driver-563");
        assertThat(groupByResult.getMaterializedRows().getFirst().getField(1)).isEqualTo(40000L);
        assertThat(groupByResult.getStatementStats().get().getPhysicalInputBytes()).isLessThan(500000L);
    }

    @Test
    public void testReadPartitionedMORTables()
    {
        getQueryRunner().execute(getSession(), "SET SESSION hudi.metadata_enabled=true");
        String res = getQueryRunner().execute(getSession(), "SELECT * FROM " + HUDI_STOCK_TICKS_MOR).toString();
        System.out.println(res);
    }

    @Test
    public void testMultiPartitionedTable()
    {
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 1",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=10', 1, 'a1', 1000, '2021-12-09', '10')");
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 2",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=11', 2, 'a2', 1000, '2021-12-09', '11')");
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE " + STOCK_TICKS_COW).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.stock_ticks_cow \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   volume bigint,\n" +
                        "   ts varchar,\n" +
                        "   symbol varchar,\n" +
                        "   year integer,\n" +
                        "   month varchar,\n" +
                        "   high double,\n" +
                        "   low double,\n" +
                        "   key varchar,\n" +
                        "   date varchar,\n" +
                        "   close double,\n" +
                        "   open double,\n" +
                        "   day varchar,\n" +
                        "   dt varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/stock_ticks_cow',\n\\Q" +
                        "   partitioned_by = ARRAY['dt']\n" +
                        ")");
        // multi-partitioned table
        assertThat((String) computeActual("SHOW CREATE TABLE " + HUDI_COW_PT_TBL).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.hudi_cow_pt_tbl \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   id bigint,\n" +
                        "   name varchar,\n" +
                        "   ts bigint,\n" +
                        "   dt varchar,\n" +
                        "   hh varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/hudi_cow_pt_tbl',\n\\Q" +
                        "   partitioned_by = ARRAY['dt','hh']\n" +
                        ")");
    }

    @Test
    public void testMetaColumns()
    {
        assertQuery("SELECT _hoodie_commit_time FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640'), ('20220906063456550')");
        assertQuery("SELECT _hoodie_commit_seqno FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640_0_0'), ('20220906063456550_0_0')");
        assertQuery("SELECT _hoodie_record_key FROM hudi_cow_pt_tbl", "VALUES ('id:1'), ('id:2')");
        assertQuery("SELECT _hoodie_partition_path FROM hudi_cow_pt_tbl", "VALUES ('dt=2021-12-09/hh=10'), ('dt=2021-12-09/hh=11')");
        assertQuery(
                "SELECT _hoodie_file_name FROM hudi_cow_pt_tbl",
                "VALUES ('719c3273-2805-4124-b1ac-e980dada85bf-0_0-27-1215_20220906063435640.parquet'), ('4a3fcb9b-65eb-4f6e-acf9-7b0764bb4dd1-0_0-70-2444_20220906063456550.parquet')");
    }

    @Test
    public void testPathColumn()
            throws Exception
    {
        String path1 = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(toInputFile(path1).exists()).isTrue();
        String path2 = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_STOCK_TICKS_MOR + " WHERE volume = 6794");
        assertThat(toInputFile(path2).exists()).isTrue();
    }

    @Test
    public void testFileSizeColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        long fileSize = (long) computeScalar("SELECT \"$file_size\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(fileSize).isEqualTo(toInputFile(path).length());
    }

    @Test
    public void testFileModifiedColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        ZonedDateTime fileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(fileModifiedTime.toInstant().toEpochMilli())
                .isEqualTo(toInputFile(path).lastModified().toEpochMilli());
    }

    @Test
    public void testPartitionColumn()
    {
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1", "VALUES 'dt=2021-12-09/hh=10'");
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 2", "VALUES 'dt=2021-12-09/hh=11'");

        assertQueryFails("SELECT \"$partition\" FROM " + HUDI_NON_PART_COW, ".* Column '\\$partition' cannot be resolved");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableStatistics(boolean tableStatisticsEnabled)
            throws InterruptedException
    {
        @Language("SQL") String query = "EXPLAIN (FORMAT JSON) SELECT t1.uuid, t1.driver, t1.fare, t1.ts "
                + "FROM " + HUDI_TRIPS_COW_V8 + " t1 "
                + "INNER JOIN " + HUDI_TRIPS_COW_V8 + " t2 ON t1.uuid = t2.uuid "
                + "WHERE t2.ts > 0";
        Session session = SessionBuilder.from(getSession())
                .withJoinDistributionType(OptimizerConfig.JoinDistributionType.AUTOMATIC.name())
                .withTableStatisticsEnabled(tableStatisticsEnabled)
                .withMdtEnabled(true)
                .build();
        QueryRunner queryRunner = getQueryRunner();
        // First time the asynchronous table statistics calculation is triggered
        queryRunner.execute(session, query);
        Thread.sleep(2000);
        // Second time the table statistics is available for CBO and the join distribution type should be REPLICATED
        String jsonPlanString = (String) queryRunner.execute(session, query).getOnlyValue();

        // Navigate to the ScanFilterProject node
        String tableName = "tests.hudi_trips_cow_v8";
        JSONObject scanNode = findNodeInPlan(jsonPlanString, "ScanFilterProject", Option.of(tableName));
        assertThat(scanNode).isNotNull();

        // Verify the estimates are based on the table statistics if enabled
        JSONArray estimatesArray = scanNode.getJSONArray("estimates");
        assertThat(estimatesArray).isNotNull();
        assertThat(estimatesArray.length()).isGreaterThan(0);
        JSONObject estimates = estimatesArray.getJSONObject(0);
        assertThat(estimates).isNotNull();
        if (tableStatisticsEnabled) {
            assertThat(estimates.getDouble("outputRowCount")).isEqualTo(40000.0);
            assertThat(estimates.getDouble("outputSizeInBytes")).isGreaterThan(20000.0);
        }
        else {
            assertThat(estimates.getString("outputRowCount")).isEqualTo("NaN");
            assertThat(estimates.getString("outputSizeInBytes")).isEqualTo("NaN");
        }

        // Verify the join distribution type is REPLICATED if table statistics is enabled; PARTITIONED otherwise
        JSONObject joinNode = findNodeInPlan(jsonPlanString, "InnerJoin", Option.empty());
        String distributionDetails = findDetailContaining(joinNode, "Distribution");
        assertThat(distributionDetails).isNotNull();
        String distribution = distributionDetails.split(":")[1].trim();
        assertThat(distribution).isEqualTo(tableStatisticsEnabled ? "REPLICATED" : "PARTITIONED");
    }

    @Test
    public void testPartitionFilterRequired()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQueryFails(
                session,
                "SELECT * FROM " + HUDI_COW_PT_TBL,
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredPredicateOnNonPartitionColumn()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQueryFails(
                session,
                "SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE id = 1",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredNestedQueryWithInnerPartitionPredicate()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQuery(session, "SELECT name FROM (SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE dt = '2021-12-09') WHERE id = 1", "VALUES 'a1'");
    }

    @Test
    public void testPartitionFilterRequiredNestedQueryWithOuterPartitionPredicate()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQuery(session, "SELECT name FROM (SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE id = 1) WHERE dt = '2021-12-09'", "VALUES 'a1'");
    }

    @Test
    public void testPartitionFilterRequiredNestedWithIsNotNullFilter()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE dt IS NOT null", "VALUES 'a1', 'a2'");
    }

    @Test
    public void testPartitionFilterRequiredFilterRemovedByPlanner()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        assertQueryFails(
                session,
                "SELECT id FROM " + HUDI_COW_PT_TBL + " WHERE dt IS NOT null OR true",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredOnJoin()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";

        // ON with partition column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt)",
                errorMessage);
        // ON with partition column and WHERE with same left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with same right table's regular column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with different left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.hh = '10'",
                "VALUES ('a1', 'a1'), ('a1', 'a2')");
        // ON with partition column and WHERE with different regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.hh = '10'",
                errorMessage);
        // ON with partition column and WHERE with regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.id = 1",
                errorMessage);

        // ON with regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.id = t2.id)",
                errorMessage);
        // ON with regular column and WHERE with left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.id = t2.id) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2')");
        // ON with partition column and WHERE with right table's regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.id = 1",
                errorMessage);
    }

    @Test
    public void testPartitionFilterRequiredOnJoinBothTablePartitioned()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();

        // ON with partition column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt)",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
        // ON with partition column and WHERE with same left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with same right table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");

        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";
        // ON with partition column and WHERE with different left table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t1.hh = '10'", errorMessage);
        // ON with partition column and WHERE with different right table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.hh = '10'", errorMessage);
        // ON with partition column and WHERE with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.id = 1", errorMessage);

        // ON with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id)", errorMessage);
        // ON with regular column and WHERE with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id) WHERE t1.id = 1", errorMessage);
        // ON with regular column and WHERE with left table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id) WHERE t1.dt = '2021-12-09'", errorMessage);
    }

    @Test
    public void testPartitionFilterRequiredWithLike()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();
        assertQueryFails(
                session,
                "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1'",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredFilterIncluded()
    {
        Session session = SessionBuilder.from(getSession())
                .withPartitionFilterRequired(true)
                .build();
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh = '10'", "VALUES 'a1'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh < '12'", "VALUES 2");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE Hh < '11'", "VALUES 1");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE HH < '10'", "VALUES 0");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 = 1 and hh IS NOT NULL", "VALUES 'a2'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh IS NULL", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh IS NOT NULL", "VALUES 2");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh LIKE '10'", "VALUES 'a1'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh LIKE '1%'", "VALUES 'a1', 'a2'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE id = 1 AND dt = '2021-12-09'", "VALUES 'a1'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh = '11' AND dt = '2021-12-09'", "VALUES 'a2'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '12' AND dt = '2021-12-19'", "VALUES 0");

        // Predicate which could not be translated into tuple domain
        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 = 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) - 11 = 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) * 2 = 20", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 > 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1' OR hh LIKE '%1'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1' AND hh LIKE '%0'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE id = 1 OR dt = '2021-12-09'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '11' OR dt = '2021-12-09'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '12' OR dt = '2021-12-19'", errorMessage);
        assertQueryFails(session, "SELECT count(*) AS COUNT FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) > 2 GROUP BY name ", errorMessage);
    }

    @Test
    public void testHudiLongTimestampType()
            throws Exception
    {
        testTimestampMicros(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestampMicros(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestampMicros(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
    }

    @Test
    public void testHudiCustomKeygenEpochMillisPartitionedTables()
    {
        // NOTE: As of now, the partition_path value that is synced to metastore will be returned instead of the raw value that is used by the keygen
        Session session = SessionBuilder.from(getSession()).build();
        @Language("SQL") String actualQuery = "SELECT _hoodie_partition_path, partition_field_country, partition_field_date FROM " + HUDI_CUSTOM_KEYGEN_PT_V8_MOR;
        @Language("SQL") String expectedQuery = "VALUES ('partition_field_country=MY/partition_field_date=2025-05-13', 'MY', '2025-05-13')," +
                "('partition_field_country=CN/partition_field_date=2025-06-05', 'CN', '2025-06-05')," +
                "('partition_field_country=US/partition_field_date=2025-06-06', 'US', '2025-06-06')," +
                "('partition_field_country=SG/partition_field_date=2025-06-06', 'SG', '2025-06-06')," +
                "('partition_field_country=SG/partition_field_date=2025-06-06', 'SG', '2025-06-06')," +
                "('partition_field_country=SG/partition_field_date=2025-06-07', 'SG', '2025-06-07')," +
                "('partition_field_country=SG/partition_field_date=2025-06-07', 'SG', '2025-06-07')";
        assertQuery(session, actualQuery, expectedQuery);

        // Ensure that partition pruning is working (using partition_path value) of level 3 partition_path value
        @Language("SQL") String actualPartitionPruningQuery = actualQuery + " WHERE partition_field_date='2025-06-06'";
        MaterializedResult partitionPruningResult = getQueryRunner().execute(session, actualPartitionPruningQuery);
        // Only one split in the partition, hence, only one split processed
        assertThat(partitionPruningResult.getStatementStats().get().getTotalSplits()).isEqualTo(2);
        // 2 splits/filegroups, but 3 rows
        assertQuery(actualPartitionPruningQuery, "VALUES ('partition_field_country=US/partition_field_date=2025-06-06', 'US', '2025-06-06'), " +
                "('partition_field_country=SG/partition_field_date=2025-06-06', 'SG', '2025-06-06'), " +
                "('partition_field_country=SG/partition_field_date=2025-06-06', 'SG', '2025-06-06')");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHudiTimestampKeygenEpochMillisPartitionedTables(boolean isRtTable)
    {
        String tableName = isRtTable ? HUDI_TIMESTAMP_KEYGEN_PT_EPOCH_TO_YYYY_MM_DD_HH_V8_MOR.getRtTableName()
                : HUDI_TIMESTAMP_KEYGEN_PT_EPOCH_TO_YYYY_MM_DD_HH_V8_MOR.getRoTableName();
        // NOTE: As of now, the partition_path value that is synced to metastore will be returned instead of the raw value that is used by the keygen
        Session session = SessionBuilder.from(getSession()).build();
        @Language("SQL") String actualQuery = "SELECT _hoodie_partition_path, partition_field FROM " + tableName;
        @Language("SQL") String expectedQuery = "VALUES ('2025-06-07 08', '2025-06-07 08'), ('2025-06-06 10', '2025-06-06 10'), ('2025-06-06 09', '2025-06-06 09'), " +
                "('2025-06-05 05', '2025-06-05 05'), ('2025-05-13 02', '2025-05-13 02')";
        assertQuery(session, actualQuery, expectedQuery);

        // Ensure that partition pruning is working (using partition_path value)
        @Language("SQL") String actualPartPruningQuery = actualQuery + " WHERE partition_field='2025-06-07 08'";
        MaterializedResult partPruneRes = getQueryRunner().execute(session, actualPartPruningQuery);
        // Only one split in the partition, hence, only one split processed
        assertThat(partPruneRes.getStatementStats().get().getTotalSplits()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHudiTimestampKeygenScalarPartitionedTables(boolean isRtTable)
    {
        String tableName = isRtTable ? HUDI_TIMESTAMP_KEYGEN_PT_SCALAR_TO_YYYY_MM_DD_HH_V8_MOR.getRtTableName()
                : HUDI_TIMESTAMP_KEYGEN_PT_SCALAR_TO_YYYY_MM_DD_HH_V8_MOR.getRoTableName();
        // NOTE: As of now, the partition_path value that is synced to metastore will be returned instead of the raw value that is used by the keygen
        Session session = SessionBuilder.from(getSession()).build();
        @Language("SQL") String actualQuery = "SELECT _hoodie_partition_path, partition_field FROM "
                + tableName;
        @Language("SQL") String expectedQuery = "VALUES ('2024-10-04 12', '2024-10-04 12'), ('2024-10-05 12', '2024-10-05 12'), ('2024-10-06 12', '2024-10-06 12'), " +
                "('2024-10-07 12', '2024-10-07 12'), ('2024-10-08 12', '2024-10-08 12')";
        assertQuery(session, actualQuery, expectedQuery);

        // Ensure that partition pruning is working (using partition_path value)
        @Language("SQL") String actualPartPruningQuery = actualQuery + " WHERE partition_field='2024-10-04 12'";
        MaterializedResult partPruneRes = getQueryRunner().execute(session, actualPartPruningQuery);
        // Only one split in the partition, hence, only one split processed
        assertThat(partPruneRes.getStatementStats().get().getTotalSplits()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testPartitionPruningReadMultiFgPartitionedMOR(ResourceHudiTablesInitializer.TestingTable table)
    {
        // Test for partition pruning without MDT (i.e. w/o partition pruning using partition stats index)
        Session session = SessionBuilder.from(getSession()).build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + table);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + table + " WHERE country='SG'");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With partition pruning, only 2 splits in the partition should be returned
        assertThat(prunedSplits).isEqualTo(2);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testColStatsFileSkipping(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(true)
                .withColumnStatsTimeout("1s")
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + table);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + table + " WHERE country='SG' AND name='a1'");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With colstats file skipping, only 1 split should be returned
        assertThat(prunedSplits).isEqualTo(1);
    }

    @Test
    public void testColStatsFileSkippingMORTable()
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(true)
                .withColumnStatsTimeout("1s")
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .build();
        MaterializedResult roTableRes = getQueryRunner().execute(session, "SELECT id, name FROM " + HUDI_NON_PART_MOR + " where name = 'Alice'");
        MaterializedResult rtTableRes = getQueryRunner().execute(session, "SELECT id, name FROM " + HUDI_NON_PART_MOR + "_rt where name = 'Cathy'");

        // verify ro table returns results from base file
        int roTableSplits = roTableRes.getStatementStats().get().getTotalSplits();
        int roTableRows = roTableRes.getRowCount();
        assertThat(roTableSplits).isEqualTo(1);
        assertThat(roTableRows).isEqualTo(1);
        assertThat(roTableRes.getMaterializedRows().getFirst().getField(1)).isEqualTo("Alice");

        // verify rt table returns results from log file
        int rtTableSplits = rtTableRes.getStatementStats().get().getTotalSplits();
        int rtTableRows = rtTableRes.getRowCount();
        assertThat(rtTableRows).isEqualTo(1);
        assertThat(rtTableSplits).isEqualTo(1);
        assertThat(rtTableRes.getMaterializedRows().getFirst().getField(1)).isEqualTo("Cathy");
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_COW_TABLE_WITH_FIELD_NAMES_IN_CAPS", "HUDI_MOR_TABLE_WITH_FIELD_NAMES_IN_CAPS"})
    public void testFileSkippingWithColumnNameUsingUppercaseLetters(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(true)
                .withColumnStatsTimeout("5s")
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .withResolveColumnNameCasingEnabled(true)
                .build();
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM  " + table + " WHERE name='Alice'");
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + table);
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int totalRows = totalRes.getRowCount();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        int prunedRows = prunedRes.getRowCount();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With colstats file skipping, only 1 split should be returned
        assertThat(prunedSplits).isEqualTo(1);
        assertThat(totalRows).isEqualTo(2);
        assertThat(prunedRows).isEqualTo(1);
    }

    @Test
    public void testRLIWithColumnNameUsingUppercaseLetters()
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(true)
                .withRecordIndexTimeout("1s")
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .withResolveColumnNameCasingEnabled(true)
                .build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + HUDI_COW_TABLE_WITH_FIELD_NAMES_IN_CAPS);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM  " + HUDI_COW_TABLE_WITH_FIELD_NAMES_IN_CAPS + " WHERE id='1'");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int totalRows = totalRes.getRowCount();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        int prunedRows = prunedRes.getRowCount();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With record index file skipping, only 1 split should be returned
        assertThat(prunedSplits).isEqualTo(1);
        assertThat(totalRows).isEqualTo(2);
        assertThat(prunedRows).isEqualTo(1);
    }

    @Test
    public void testMultiKeyRLIWithColumnNameUsingUppercaseLetters()
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(true)
                .withRecordIndexTimeout("1s")
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .withResolveColumnNameCasingEnabled(true)
                .build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + HUDI_COW_TABLE_WITH_MULTI_KEYS_AND_FIELD_NAMES_IN_CAPS);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM  " + HUDI_COW_TABLE_WITH_MULTI_KEYS_AND_FIELD_NAMES_IN_CAPS + " WHERE id='1' and age=30");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int totalRows = totalRes.getRowCount();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        int prunedRows = prunedRes.getRowCount();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With record index file skipping, only 1 split should be returned
        assertThat(prunedSplits).isEqualTo(1);
        assertThat(totalRows).isEqualTo(2);
        assertThat(prunedRows).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testRecordLevelFileSkipping(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(true)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(false)
                .withColumnStatsTimeout("1s")
                .build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + table);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + table
                + " WHERE country='SG' AND id IN (1, 3) AND name = 'a1'");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // With RLI file skipping, only 1 split should be returned
        assertThat(prunedSplits).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testSecondaryIndexFileSkipping(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(true)
                .withPartitionStatsIndexEnabled(false)
                .withSecondaryIndexTimeout("10s")
                .build();
        MaterializedResult totalRes = getQueryRunner().execute(session, "SELECT * FROM " + table);
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + table
                + " WHERE country='SG' AND price = 101.00");
        int totalSplits = totalRes.getStatementStats().get().getTotalSplits();
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();
        assertThat(prunedSplits).isLessThan(totalSplits);
        // SI is only available for table versions >= 8
        // With SI file skipping, only 1 split should be returned
        int expectedSplits = table.getHoodieTableVersion()
                .greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 1 : 2;
        assertThat(prunedSplits).isEqualTo(expectedSplits);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testPartitionStatsIndexPartitionPruning(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(true)
                .build();
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + table
                // Add a constraint that is in colstats
                + " WHERE ts < 1001 " +
                // Add a constraint that is in colstats
                "AND price < 200.00 " +
                // Add a constraint on a column that is not in colstats
                "AND _hoodie_file_name = 'abc' " +
                // Add a simple null check constraint
                "AND id is not null");
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();

        // Partition stats index is only available for table versions >= 8
        // With PSI, only 2 splits in the SG partitions will be scanned
        int expectedSplits = table.getHoodieTableVersion()
                .greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 2 : 4;
        assertThat(prunedSplits).isEqualTo(expectedSplits);
    }

    @Test
    public void testPartitionStatsIndexPartitionPruningWithColumnNameUsingUppercaseLetters()
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .withColStatsIndexEnabled(false)
                .withRecordLevelIndexEnabled(false)
                .withSecondaryIndexEnabled(false)
                .withPartitionStatsIndexEnabled(true)
                .build();
        MaterializedResult prunedRes = getQueryRunner().execute(session, "SELECT * FROM " + HUDI_COW_PT_TABLE_WITH_FIELD_NAMES_IN_CAPS
                + " WHERE country = 'US'");
        int prunedSplits = prunedRes.getStatementStats().get().getTotalSplits();

        assertThat(prunedSplits).isEqualTo(2);
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testDynamicFilterEnabledPredicatePushdown(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder
                .from(getSession())
                .withDynamicFilterTimeout("10s")
                .build();
        final String tableIdentifier = "hudi:tests." + table.getRoTableName();

        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts FROM " +
                table + " t1 " +
                "INNER JOIN " + table + " t2 ON t1.id = t2.id " +
                "WHERE t2.price <= 102";
        MaterializedResult explainRes = getQueryRunner().execute(session, "EXPLAIN ANALYZE " + query);
        Pattern scanFilterInputRowsPattern = getScanFilterInputRowsPattern(tableIdentifier);
        Matcher matcher = scanFilterInputRowsPattern.matcher(explainRes.toString());
        assertThat(matcher.find())
                .withFailMessage("Could not find 'ScanFilter' for table '%s' with 'dynamicFilters' and 'Input: X rows' stats in EXPLAIN output.\nOutput was:\n%s",
                        tableIdentifier, explainRes.toString())
                .isTrue();

        // matcher#group() must be invoked after matcher#find()
        String rowsInputString = matcher.group(1);
        long actualInputRows = Long.parseLong(rowsInputString);
        long expectedInputRowsAfterFiltering = 2;
        assertThat(actualInputRows)
                .describedAs("Number of rows input to the ScanFilter for the probe side table (%s) should reflect effective dynamic filtering", tableIdentifier)
                .isEqualTo(expectedInputRowsAfterFiltering);

        // Exercise query and check output
        assertQuery(query, "VALUES (1, 'a1', 100.0, 1000), (3, 'a3', 101.0, 1001)");
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testDynamicFilterDisabledPredicatePushdown(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withDynamicFilterEnabled(false)
                .build();
        final String tableIdentifier = "hudi:tests." + table.getRoTableName();

        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts FROM " +
                table + " t1 " +
                "INNER JOIN " + table + " t2 ON t1.id = t2.id " +
                "WHERE t2.price <= 102";
        MaterializedResult explainRes = getQueryRunner().execute(session, "EXPLAIN ANALYZE " + query);
        Pattern scanFilterInputRowsPattern = getScanFilterInputRowsPattern(tableIdentifier);
        Matcher matcher = scanFilterInputRowsPattern.matcher(explainRes.toString());
        // Should not find any dynamic filtering keywords
        assertThat(matcher.find())
                .withFailMessage("Could not find 'ScanFilter' for table '%s' with 'dynamicFilters' and 'Input: X rows' stats in EXPLAIN output.\nOutput was:\n%s",
                        tableIdentifier, explainRes.toString())
                .isFalse();

        // Skip check on whether optimization is not applied or not, just check that output is queryable
        assertQuery(query, "VALUES (1, 'a1', 100.0, 1000), (3, 'a3', 101.0, 1001)");
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testDynamicFilterEnabled_withPartitionPruningUsingDynamicFilter(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withDynamicFilterTimeout("10s")
                .build();
        final String tableIdentifier = "hudi:tests." + table.getRoTableName();
        // Query is joined-on partitionField
        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts, t1.country FROM " +
                table + " t1 " +
                "INNER JOIN " + table + " t2 ON t1.country = t2.country " +
                "WHERE t2.price <= 102";

        MaterializedResult explainRes = getQueryRunner().execute(session, "EXPLAIN ANALYZE " + query);
        Pattern scanFilterInputRowsPattern = getScanFilterInputRowsPattern(tableIdentifier);
        Matcher matcher = scanFilterInputRowsPattern.matcher(explainRes.toString());
        assertThat(matcher.find())
                .withFailMessage("Could not find 'ScanFilter' for table '%s' with 'dynamicFilters' and 'Input: X rows' stats in EXPLAIN output.\nOutput was:\n%s",
                        tableIdentifier, explainRes.toString())
                .isTrue();

        // matcher#group() must be invoked after matcher#find()
        String rowsInputString = matcher.group(1);
        long actualInputRows = Long.parseLong(rowsInputString);
        long expectedInputRowsAfterFiltering = 2;
        assertThat(actualInputRows)
                .describedAs("Number of rows input to the ScanFilter for the probe side table (%s) should reflect effective dynamic filtering", tableIdentifier)
                .isEqualTo(expectedInputRowsAfterFiltering);

        // Exercise query and check output
        assertQuery(query, "VALUES (1, 'a1', 100.0, 1000, 'SG'), (3, 'a3', 101.0, 1001, 'SG'), (1, 'a1', 100.0, 1000, 'SG'), (3, 'a3', 101.0, 1001, 'SG')");
    }

    @Test
    public void testDynamicFilterEnabled_withPartitionPruningUsingDynamicFilterOnNestedPartitions()
    {
        Session session = SessionBuilder.from(getSession())
                .withDynamicFilterTimeout("10s")
                .build();
        final String tableIdentifier = "hudi:tests." + HUDI_MULTI_PT_V8_MOR.getRoTableName();
        // Query is joined-on recordKey and partitionField
        @Language("SQL") String query = "SELECT t1.id FROM " +
                HUDI_MULTI_PT_V8_MOR + " t1 " +
                "INNER JOIN " + HUDI_MULTI_PT_V8_MOR + " t2 ON t1.id = t2.id AND t1.part_int = t2.part_int " +
                "WHERE t2.part_int = 2023";

        MaterializedResult explainRes = getQueryRunner().execute(session, "EXPLAIN ANALYZE " + query);
        Pattern scanFilterInputRowsPattern = getScanFilterInputRowsPattern(tableIdentifier);
        Matcher matcher = scanFilterInputRowsPattern.matcher(explainRes.toString());
        assertThat(matcher.find())
                .withFailMessage("Could not find 'ScanFilter' for table '%s' with 'dynamicFilters' and 'Input: X rows' stats in EXPLAIN output.\nOutput was:\n%s",
                        tableIdentifier, explainRes.toString())
                .isTrue();

        // matcher#group() must be invoked after matcher#find()
        String rowsInputString = matcher.group(1);
        long actualInputRows = Long.parseLong(rowsInputString);
        // 1 row in each split, should only scan 3 splits, i.e. 3 rows
        // For a more strict search, we can check the number of splits scanned on the builder side
        long expectedInputRowsAfterFiltering = 3;
        assertThat(actualInputRows)
                .describedAs("Number of rows input to the ScanFilter for the probe side table (%s) should reflect effective dynamic filtering", tableIdentifier)
                .isEqualTo(expectedInputRowsAfterFiltering);

        // Exercise query and check output
        assertQuery(query, "VALUES (1), (2), (4)");
    }

    @ParameterizedTest
    @EnumSource(
            value = ResourceHudiTablesInitializer.TestingTable.class,
            names = {"HUDI_MULTI_FG_PT_V6_MOR", "HUDI_MULTI_FG_PT_V8_MOR"})
    public void testDynamicFilterDisabled_withPartitionPruningUsingDynamicFilter(ResourceHudiTablesInitializer.TestingTable table)
    {
        Session session = SessionBuilder.from(getSession())
                .withDynamicFilterEnabled(false)
                .build();
        final String tableIdentifier = "hudi:tests." + table.getRoTableName();

        // Query is joined-on recordKey and partitionField
        @Language("SQL") String query = "SELECT t1.id, t1.name, t1.price, t1.ts, t1.country FROM " +
                table + " t1 " +
                "INNER JOIN " + table + " t2 ON t1.country = t2.country " +
                "WHERE t2.price <= 102";

        MaterializedResult explainRes = getQueryRunner().execute(session, "EXPLAIN ANALYZE " + query);
        Pattern scanFilterInputRowsPattern = getScanFilterInputRowsPattern(tableIdentifier);
        Matcher matcher = scanFilterInputRowsPattern.matcher(explainRes.toString());
        assertThat(matcher.find())
                .withFailMessage("Could not find 'ScanFilter' for table '%s' with 'dynamicFilters' and 'Input: X rows' stats in EXPLAIN output.\nOutput was:\n%s",
                        tableIdentifier, explainRes.toString())
                .isFalse();

        // Skip check on whether optimization is not applied or not, just check that output is queryable
        // Cartesian product of result is produced since we are joining by partition column
        assertQuery(query, "VALUES (1, 'a1', 100.0, 1000, 'SG'), (3, 'a3', 101.0, 1001, 'SG'), (1, 'a1', 100.0, 1000, 'SG'), (3, 'a3', 101.0, 1001, 'SG')");
    }

    @Test
    public void testPartitionPruningOnNestedPartitions()
    {
        // Should only scan paths that match the part_str=*/part_int=2023/part_date=*/part_bigint=*/part_decimal=*/part_timestamp=*/part_bool=*
        Session session = getSession();
        // No partition pruning
        @Language("SQL") String actualQuery = "SELECT part_str, part_int, part_date, part_bigint, part_bool FROM " + HUDI_MULTI_PT_V8_MOR;
        MaterializedResult actualRes = getQueryRunner().execute(session, actualQuery);
        int actualTotalSplits = actualRes.getStatementStats().get().getTotalSplits();
        assertThat(actualTotalSplits).isEqualTo(5);

        // With partition pruning
        @Language("SQL") String actualPartPruneQuery = actualQuery + " WHERE part_int = 2023";
        MaterializedResult actualPartPruneRes = getQueryRunner().execute(session, actualPartPruneQuery);
        int actualPartPruneSplits = actualPartPruneRes.getStatementStats().get().getTotalSplits();
        assertThat(actualPartPruneSplits).isLessThan(actualTotalSplits);
        assertThat(actualPartPruneSplits).isEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("comprehensiveTestParameters")
    public void testComprehensiveTypes(ResourceHudiTablesInitializer.TestingTable table, boolean isRtTable)
    {
        Session session = SessionBuilder.from(getSession())
                .withMdtEnabled(true)
                .build();
        // Not using #assertQuery() as it uses H2QueryRunner, which restricts the types which can be defined, particularly MAP types
        // Use #getQueryRunner(), which uses TrinoQueryRunner instead

        // Define the columns that are being tested:
        ImmutableList<String> columnsToTest = ImmutableList.of(
                "uuid", "precombine_field", "col_boolean", "col_tinyint", "col_smallint", "col_int", "col_bigint", "col_float", "col_double", "col_decimal", "col_string",
                "col_varchar", "col_char", "col_binary", "col_date", "col_timestamp", "col_array_int", "col_array_string", "col_map_string_int", "col_struct", "col_array_struct",
                "col_map_string_struct", "col_array_struct_with_map", "col_map_struct_with_array", "col_struct_nested_struct", "col_array_array_int", "col_map_string_array_double",
                "col_map_string_map_string_date", "col_struct_array_struct", "col_struct_map", "part_col");

        // Define expected output
        ImmutableList<ImmutableList<String>> expectedRowValues = ImmutableList.of(
                // uuid STRING
                ImmutableList.of("'uuid1'", "'uuid2'", "'uuid3'"),
                // precombine_field LONG -> BIGINT
                ImmutableList.of(
                        // Updates were performed (RT table holds the updated value)
                        isRtTable ? "BIGINT '1001'" : "BIGINT '1000'",
                        isRtTable ? "BIGINT '1006'" : "BIGINT '1005'",
                        isRtTable ? "BIGINT '1101'" : "BIGINT '1100'"),
                // col_boolean BOOLEAN
                ImmutableList.of("true", "false", "CAST(NULL AS BOOLEAN)"),
                // col_tinyint TINYINT
                ImmutableList.of("TINYINT '1'", "TINYINT '2'", "CAST(NULL AS TINYINT)"),
                // col_smallint SMALLINT
                ImmutableList.of("SMALLINT '100'", "SMALLINT '200'", "CAST(NULL AS SMALLINT)"),
                // col_int
                ImmutableList.of("INTEGER '1000'", "INTEGER '2000'", "CAST(NULL AS INTEGER)"),
                // col_bigint BIGINT
                ImmutableList.of("BIGINT '100000'", "BIGINT '200000'", "CAST(NULL AS BIGINT)"),
                // col_float
                ImmutableList.of("REAL '1.1'", "REAL '2.2'", "CAST(NULL AS REAL)"),
                // col_double
                ImmutableList.of(
                        // Updates were performed on partition A values (RT table holds the updated value)
                        isRtTable ? "DOUBLE '110.123'" : "DOUBLE '10.123'",
                        isRtTable ? "DOUBLE '120.456'" : "DOUBLE '20.456'",
                        "CAST(NULL AS DOUBLE)"),
                // col_decimal
                ImmutableList.of("DECIMAL '123.45'", "DECIMAL '234.56'", "CAST(NULL AS DECIMAL(10,2))"),
                // col_string
                ImmutableList.of(
                        "'string val 1'",
                        "'string val 2'",
                        // Updates were performed on partition B values (RT table holds the updated value)
                        isRtTable ? "'updated string'" : "NULL"),
                // col_varchar
                ImmutableList.of("CAST('varchar val 1' AS VARCHAR(50))", "CAST('varchar val 2' AS VARCHAR(50))", "CAST(NULL AS VARCHAR(50))"),
                // col_char
                ImmutableList.of("CAST('charval1' AS CHAR(10))", "CAST('charval2' AS CHAR(10))", "CAST(NULL AS CHAR(10))"),
                // col_binary BINARY -> VARBINARY: UTF-8 bytes of "binary1", "binary2", null
                ImmutableList.of("X'62696e61727931'", "X'62696e61727932'", "CAST(NULL AS VARBINARY)"),
                // col_date
                ImmutableList.of("DATE '2025-01-15'", "DATE '2025-02-20'", "CAST(NULL AS DATE)"),
                // col_timestamp TIMESTAMP
                ImmutableList.of("TIMESTAMP '2025-01-15 11:30:00.000'", "TIMESTAMP '2025-02-20 12:45:00.000'", "CAST(NULL AS TIMESTAMP)"),
                // col_array_int ARRAY<INT>
                ImmutableList.of("ARRAY[1, 2, 3]", "ARRAY[4, 5]", "CAST(NULL AS ARRAY<INTEGER>)"),
                // col_array_string ARRAY<STRING>
                ImmutableList.of("ARRAY['a', 'b', 'c']", "ARRAY['d', 'e', 'f']", "CAST(NULL AS ARRAY<VARCHAR>)"),
                // col_map_string_int MAP<STRING, INT>
                ImmutableList.of("MAP(ARRAY['key1', 'key2'], ARRAY[10, 20])", "MAP(ARRAY['key3'], ARRAY[30])", "CAST(NULL AS MAP(VARCHAR, INTEGER))"),
                // col_struct
                ImmutableList.of(
                        "CAST(ROW('struct_str1', 55, false) AS ROW(f1 VARCHAR, f2 INTEGER, f3 BOOLEAN))",
                        "CAST(ROW('struct_str2', 66, true) AS ROW(f1 VARCHAR, f2 INTEGER, f3 BOOLEAN))",
                        "CAST(NULL AS ROW(f1 VARCHAR, f2 INTEGER, f3 BOOLEAN))"),
                // col_array_struct
                ImmutableList.of(
                        "ARRAY[CAST(ROW(1.1E0, ARRAY['n1','n2']) AS ROW(nested_f1 DOUBLE, nested_f2 ARRAY<VARCHAR>)), CAST(ROW(2.2E0, ARRAY['n3']) AS ROW(nested_f1 DOUBLE, nested_f2 ARRAY<VARCHAR>))]",
                        "CAST(NULL AS ARRAY<ROW(nested_f1 DOUBLE, nested_f2 ARRAY<VARCHAR>)>)",
                        "ARRAY[CAST(ROW(3.3E0, ARRAY['n4']) AS ROW(nested_f1 DOUBLE, nested_f2 ARRAY<VARCHAR>))]"),
                // col_map_string_struct
                ImmutableList.of(
                        "MAP(ARRAY['mapkey1'], ARRAY[CAST(ROW(DATE '2024-11-01', DECIMAL '9.80') AS ROW(nested_f3 DATE, nested_f4 DECIMAL(5,2)))])",
                        "MAP(ARRAY['mapkey2'], ARRAY[CAST(ROW(DATE '2024-12-10', DECIMAL '7.60') AS ROW(nested_f3 DATE, nested_f4 DECIMAL(5,2)))])",
                        "CAST(NULL AS MAP<VARCHAR, ROW(nested_f3 DATE, nested_f4 DECIMAL(5,2))>)"),
                // col_array_struct_with_map
                ImmutableList.of(
                        "ARRAY[CAST(ROW('arr_struct1', MAP(ARRAY['map_in_struct_k1'], ARRAY[1])) AS ROW(f_arr_struct_str VARCHAR, f_arr_struct_map MAP<VARCHAR, INTEGER>)), CAST(ROW('arr_struct2', MAP(ARRAY['map_in_struct_k2', 'map_in_struct_k3'], ARRAY[2, 3])) AS ROW(f_arr_struct_str VARCHAR, f_arr_struct_map MAP<VARCHAR, INTEGER>))]",
                        // inner map is null
                        "ARRAY[CAST(ROW('arr_struct3', MAP(ARRAY['map_in_struct_k4'], ARRAY[4])) AS ROW(f_arr_struct_str VARCHAR, f_arr_struct_map MAP<VARCHAR, INTEGER>)), CAST(ROW('arr_struct4', CAST(NULL AS MAP<VARCHAR, INTEGER>)) AS ROW(f_arr_struct_str VARCHAR, f_arr_struct_map MAP<VARCHAR, INTEGER>))]",
                        "CAST(NULL AS ARRAY<ROW(f_arr_struct_str VARCHAR, f_arr_struct_map MAP<VARCHAR, INTEGER>)>)"),
                // col_map_struct_with_array
                ImmutableList.of(
                        "MAP(ARRAY['map_struct1', 'map_struct2'], ARRAY[CAST(ROW(ARRAY[true, false], TIMESTAMP '2025-01-01 01:01:01.000') AS ROW(f_map_struct_arr ARRAY<BOOLEAN>, f_map_struct_ts TIMESTAMP(3))), CAST(ROW(ARRAY[false], TIMESTAMP '2025-02-02 02:02:02.000') AS ROW(f_map_struct_arr ARRAY<BOOLEAN>, f_map_struct_ts TIMESTAMP(3)))])",
                        // inner map is null
                        "MAP(ARRAY['map_struct3', 'map_struct4'], ARRAY[CAST(ROW(CAST(NULL AS ARRAY<BOOLEAN>), TIMESTAMP '2025-03-03 03:03:03.000') AS ROW(f_map_struct_arr ARRAY<BOOLEAN>, f_map_struct_ts TIMESTAMP(3))), CAST(ROW(ARRAY[true], CAST(NULL AS TIMESTAMP(3))) AS ROW(f_map_struct_arr ARRAY<BOOLEAN>, f_map_struct_ts TIMESTAMP(3)))])",
                        "CAST(NULL AS MAP<VARCHAR, ROW(f_map_struct_arr ARRAY<BOOLEAN>, f_map_struct_ts TIMESTAMP(3))>)"),
                // col_struct_nested_struct
                ImmutableList.of(
                        "CAST(ROW(101, CAST(ROW('inner_str_1', true) AS ROW(inner_f1 VARCHAR, inner_f2 BOOLEAN))) AS ROW(outer_f1 INTEGER, nested_struct ROW(inner_f1 VARCHAR, inner_f2 BOOLEAN)))",
                        // inner struct is null
                        "CAST(ROW(102, CAST(NULL AS ROW(inner_f1 VARCHAR, inner_f2 BOOLEAN))) AS ROW(outer_f1 INTEGER, nested_struct ROW(inner_f1 VARCHAR, inner_f2 BOOLEAN)))",
                        "CAST(NULL AS ROW(outer_f1 INTEGER, nested_struct ROW(inner_f1 VARCHAR, inner_f2 BOOLEAN)))"),
                // col_array_array_int
                ImmutableList.of("ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]", "ARRAY[ARRAY[6], ARRAY[7, 8]]", "CAST(NULL AS ARRAY<ARRAY<INTEGER>>)"),
                // col_map_string_array_double
                ImmutableList.of(
                        "MAP(ARRAY['arr_key1', 'arr_key2'], ARRAY[ARRAY[1.1E0, 2.2E0], ARRAY[3.3E0]])",
                        // inner array is null
                        "MAP(ARRAY['arr_key3'], ARRAY[CAST(NULL AS ARRAY<DOUBLE>)])",
                        "CAST(NULL AS MAP<VARCHAR, ARRAY<DOUBLE>>)"),
                // col_map_string_map_string_date
                ImmutableList.of(
                        "MAP(ARRAY['map_key1'], ARRAY[MAP(ARRAY['mapkey10', 'mapkey20'], ARRAY[DATE '2024-01-01', DATE '2024-02-02'])])",
                        // inner map value/map is null, assuming int key 30 coerced to '30'
                        "MAP(ARRAY['map_key2', 'map_key3'], ARRAY[MAP(ARRAY[CAST('30' AS VARCHAR)], ARRAY[CAST(NULL AS DATE)]), CAST(NULL AS MAP<VARCHAR, DATE>)])",
                        "CAST(NULL AS MAP<VARCHAR, MAP<VARCHAR, DATE>>)"),
                // col_struct_array_struct
                ImmutableList.of(
                        "CAST(ROW('outer_str_1', ARRAY[CAST(ROW(TIMESTAMP '2023-11-11 11:11:11.000', 'inner_str_1') AS ROW(inner_f3 TIMESTAMP(3), inner_f4 VARCHAR))]) AS ROW(outer_f2 VARCHAR, struct_array ARRAY<ROW(inner_f3 TIMESTAMP(3), inner_f4 VARCHAR)>))",
                        "CAST(ROW('outer_str_2', ARRAY[CAST(ROW(TIMESTAMP '2023-12-12 12:12:12.000', 'inner_str_2') AS ROW(inner_f3 TIMESTAMP(3), inner_f4 VARCHAR))]) AS ROW(outer_f2 VARCHAR, struct_array ARRAY<ROW(inner_f3 TIMESTAMP(3), inner_f4 VARCHAR)>))",
                        "CAST(NULL AS ROW(outer_f2 VARCHAR, struct_array ARRAY<ROW(inner_f3 TIMESTAMP(3), inner_f4 VARCHAR)>))"),
                // col_struct_map (BIGINT literals don't need L)
                ImmutableList.of(
                        "CAST(ROW(true, MAP(ARRAY['struct_map_k1', 'struct_map_k2'], ARRAY[1000, 2000])) AS ROW(outer_f3 BOOLEAN, struct_map MAP<VARCHAR, BIGINT>))",
                        // inner map is null
                        "CAST(ROW(false, CAST(NULL AS MAP<VARCHAR, BIGINT>)) AS ROW(outer_f3 BOOLEAN, struct_map MAP<VARCHAR, BIGINT>))",
                        "CAST(NULL AS ROW(outer_f3 BOOLEAN, struct_map MAP<VARCHAR, BIGINT>))"),
                // part_col
                ImmutableList.of("'A'", "'A'", "'B'"));

        // "Zip" results up for convenient lookup
        Map<String, ImmutableList<String>> mapping = listsToMap(columnsToTest, expectedRowValues);

        // Determine which table to use base on test parameters
        final String sourceTable = isRtTable ? table.getRtTableName() : table.getTableName();

        // Test each column individually so that errors thrown are more specific/useful
        for (String column : mapping.keySet()) {
            // Use UNION ALL so that de-dupes will not happen
            @Language("SQL") String expectedQuery = mapping.get(column).stream().map(l -> "SELECT " + l).collect(Collectors.joining(" UNION ALL "));
            @Language("SQL") String actualQuery = "SELECT " + column + " FROM " + sourceTable;

            MaterializedResult actualResults = getQueryRunner().execute(session, actualQuery);
            MaterializedResult expectedResults = getQueryRunner().execute(session, expectedQuery);
            assertThat(actualResults.getMaterializedRows())
                    .describedAs("failedQuery: " + actualQuery)
                    .hasSameSizeAs(expectedResults.getMaterializedRows())
                    .containsAll(expectedResults.getMaterializedRows());
        }

        // Perform same test on all columns together
        int numRows = expectedRowValues.getFirst().size();
        @Language("SQL") String expectedQuery = IntStream.range(0, numRows)
                .mapToObj(rowIndex -> {
                    // For each row, collect the corresponding values for all columns in the defined order
                    String rowValuesString = columnsToTest.stream()
                            .map(columnName -> {
                                List<String> columnData = mapping.get(columnName);
                                return columnData.get(rowIndex);
                            })
                            .collect(Collectors.joining(", ")); // Joins column values: "val1, val2, val3"
                    return "SELECT " + rowValuesString; // Forms: "SELECT val1, val2, val3"
                })
                .collect(Collectors.joining(" UNION ALL "));
        @Language("SQL") String actualQuery = "SELECT " + String.join(", ", columnsToTest) + " FROM " + sourceTable;
        MaterializedResult actualResults = getQueryRunner().execute(session, actualQuery);
        MaterializedResult expectedResults = getQueryRunner().execute(session, expectedQuery);
        assertThat(actualResults.getMaterializedRows())
                .hasSameSizeAs(expectedResults.getMaterializedRows())
                .containsAll(expectedResults.getMaterializedRows());

        // Perform test on selecting nested field
        String columnToTest = "col_map_string_struct";
        // 1. Extract all values from the map into an array. Since each map has one entry, this array will have one ROW (or be NULL if the map is NULL).
        // 2. Access the first (and only) ROW object from this array. (Using 1-based indexing for arrays, which Trino and Presto uses)
        // 3. Access the 'nested_f4' field from that ROW object.
        @Language("SQL") String nestedFieldQuery = "SELECT (map_values(" + columnToTest + "))[1].nested_f4 AS extracted_nested_f4 FROM " + sourceTable;
        @Language("SQL") String expectedNestedFieldQuery = "WITH " + sourceTable + " AS ( "
                + mapping.get(columnToTest).stream()
                .map(l -> "SELECT " + l + " AS " + columnToTest)
                .collect(Collectors.joining(" UNION ALL "))
                + ") " +
                nestedFieldQuery;
        @Language("SQL") String actualNestedFieldQuery = nestedFieldQuery;
        MaterializedResult expectedNestedResult = getQueryRunner().execute(session, expectedNestedFieldQuery);
        MaterializedResult actualNestedResult = getQueryRunner().execute(session, actualNestedFieldQuery);
        assertThat(actualNestedResult.getMaterializedRows())
                .hasSameSizeAs(expectedNestedResult.getMaterializedRows())
                .containsAll(expectedNestedResult.getMaterializedRows());
    }

    @Test
    public void testHudiPartitionFieldsWithMultipleTypes()
    {
        Session session = getSession();
        @Language("SQL") String actualQuery = "SELECT part_str, part_int, part_date, part_bigint, part_bool FROM " + HUDI_MULTI_PT_V8_MOR;
        @Language("SQL") String expectedQuery = "VALUES " +
                "('apparel', 2024, DATE '2024-01-05', 20000000001, false), " +
                "('books', 2023, DATE '2023-01-15', 10000000001, true), " +
                "('books', 2024, DATE '2024-02-20', 10000000003, true), " +
                "('electronics', 2023, DATE '2023-03-10', 10000000002, false), " +
                "('electronics', 2023, DATE '2023-03-10', 10000000002, true) ";
        assertQuery(session, actualQuery, expectedQuery);
    }

    private void testTimestampMicros(HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("long_timestamp.parquet").toURI());
        Type columnType = createTimestampType(timestampPrecision.getPrecision());
        HudiSplit hudiSplit = new HudiSplit(
                new HudiBaseFile(parquetFile.getPath(), parquetFile.getName(), parquetFile.length(), parquetFile.lastModified(), 0, parquetFile.length()),
                ImmutableList.of(),
                "000",
                TupleDomain.all(),
                ImmutableList.of(),
                SplitWeight.standard());

        HudiConfig config = new HudiConfig().setUseParquetColumnNames(false);
        HudiSessionProperties sessionProperties = new HudiSessionProperties(config, new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();

        try (ConnectorPageSource pageSource = createPageSource(
                session,
                List.of(createBaseColumn("created", 0, HIVE_TIMESTAMP, columnType, REGULAR, Optional.empty())),
                hudiSplit,
                new LocalInputFile(parquetFile),
                new FileFormatDataSourceStats(),
                new ParquetReaderOptions(),
                DateTimeZone.UTC, DynamicFilter.EMPTY, true)) {
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected)));
        }
    }

    private static Pattern getScanFilterInputRowsPattern(String tableIdentifier)
    {
        // Regex to find the ScanFilter for the specific table that received a dynamic filter and extract the 'Input: X rows' value associated with it.
        return Pattern.compile(
                // Match the ScanFilter line for the specific table, ensuring dynamicFilters is present
                "ScanFilter\\[table = " + Pattern.quote(tableIdentifier) + ".*dynamicFilters = \\{.*?\\}.*?\\]" +
                        ".*?" + // Match subsequent lines non-greedily until the target line is found
                        "\\n\\s+Input:\\s+(\\d+)\\s+rows", // Match the 'Input: X rows' line, ensuring it's indented relative to ScanFilter
                Pattern.DOTALL);
    }

    private TrinoInputFile toInputFile(String path)
    {
        return ((HudiConnector) getDistributedQueryRunner().getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"))
                .newInputFile(Location.of(path));
    }

    public static <K, V> Map<K, V> listsToMap(List<K> keys, List<V> values)
    {
        if (keys == null || values == null) {
            throw new IllegalArgumentException("Key and Value lists cannot be null.");
        }

        // Determine the number of entries based on the shorter list
        int limit = Math.min(keys.size(), values.size());

        return IntStream.range(0, limit)
                .boxed()
                .collect(Collectors.toMap(
                        keys::get,
                        values::get,
                        // Merge function for duplicate keys, last one wins
                        (_, newValue) -> newValue));
    }

    private static Stream<Arguments> comprehensiveTestParameters()
    {
        ResourceHudiTablesInitializer.TestingTable[] tablesToTest = {
                HUDI_COMPREHENSIVE_TYPES_V6_MOR,
                HUDI_COMPREHENSIVE_TYPES_V8_MOR
        };
        Boolean[] booleanValues = {true, false};

        return Stream.of(tablesToTest)
                .flatMap(table ->
                        Stream.of(booleanValues)
                                .map(boolValue -> Arguments.of(table, boolValue)));
    }

    /**
     * Entry point for finding a node in the complete JSON plan string.
     * It iterates through each plan fragment ("0", "1", etc.) and starts the recursive search.
     *
     * @param jsonPlanString the complete JSON string of the execution plan
     * @param nodeType the "name" of the node type to find (e.g., "InnerJoin", "ScanFilterProject")
     * @param tableName the name of the table to match for nodes like "ScanFilterProject". can be empty
     * @return The found JSONObject, or null if not found in any fragment.
     */
    public static JSONObject findNodeInPlan(String jsonPlanString, String nodeType, Option<String> tableName)
    {
        JSONObject fullPlan = new JSONObject(jsonPlanString);

        // Iterate over the fragment keys ("0", "1", etc.)
        Iterator<String> fragmentKeys = fullPlan.keys();
        while (fragmentKeys.hasNext()) {
            String key = fragmentKeys.next();
            JSONObject fragmentNode = fullPlan.getJSONObject(key);

            // Start the recursive search from the root node of the fragment
            JSONObject result = findNodeRecursive(fragmentNode, nodeType, tableName);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Recursively searches for a node within a plan tree starting from the given node.
     *
     * @param currentNode the current {@link JSONObject} node in the plan tree to inspect
     * @param nodeType the "name" of the node type to find
     * @param tableName the table name to match for specific node types. can be empty
     * @return the found {@link JSONObject}, or null if not found.
     */
    private static JSONObject findNodeRecursive(JSONObject currentNode, String nodeType, Option<String> tableName)
    {
        if (currentNode.has("name") && currentNode.getString("name").equals(nodeType)) {
            // If a tableName is specified, we must match it (for Scan/Filter/Project nodes)
            if (tableName.isPresent()) {
                if (currentNode.has("descriptor") && currentNode.getJSONObject("descriptor").has("table")) {
                    String table = currentNode.getJSONObject("descriptor").getString("table");
                    if (table.contains(tableName.get())) {
                        return currentNode;
                    }
                }
            }
            else {
                // If no tableName is required, found a match by nodeType alone
                return currentNode;
            }
        }

        // If not a match, recurse into the children
        if (currentNode.has("children")) {
            JSONArray children = currentNode.getJSONArray("children");
            for (int i = 0; i < children.length(); i++) {
                JSONObject childNode = children.getJSONObject(i);
                JSONObject result = findNodeRecursive(childNode, nodeType, tableName);
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    /**
     * Searches the "details" array of a given plan node for a string containing specific text.
     *
     * @param node the {@link JSONObject} plan node to search within
     * @param content the substring to search for in the details array
     * @return the full text of the first matching detail, or null if no match is found.
     */
    public static String findDetailContaining(JSONObject node, String content)
    {
        if (node != null && node.has("details")) {
            JSONArray details = node.getJSONArray("details");
            for (int i = 0; i < details.length(); i++) {
                String detailText = details.getString(i);
                if (detailText.contains(content)) {
                    return detailText;
                }
            }
        }
        return null;
    }
}
