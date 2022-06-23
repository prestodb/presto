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
package com.facebook.presto.pinot;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.TestingConnectorSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPinotBrokerPageSourcePql
        extends TestPinotQueryBase
{
    private static PinotTableHandle pinotTable = new PinotTableHandle("connId", "schema", "tbl");
    private final ObjectMapper objectMapper = new JsonObjectMapperProvider().get();
    private static PinotColumnHandle jobState = new PinotColumnHandle("jobState", VARCHAR, PinotColumnHandle.PinotColumnType.REGULAR);

    private static class PqlParsedInfo
    {
        final int groupByColumns;
        final int columns;
        final int rows;

        private PqlParsedInfo(int groupByColumns, int columns, int rows)
        {
            this.groupByColumns = groupByColumns;
            this.columns = columns;
            this.rows = rows;
        }

        public static PqlParsedInfo forSelection(int columns, int rows)
        {
            return new PqlParsedInfo(0, columns, rows);
        }

        public static PqlParsedInfo forAggregation(int groups, int aggregates, int rows)
        {
            return new PqlParsedInfo(groups, groups + aggregates, rows);
        }
    }

    PqlParsedInfo getBasicInfoFromPql(String pqlResponse)
            throws IOException
    {
        JsonNode pqlJson = objectMapper.readTree(pqlResponse);
        JsonNode selectionResults = pqlJson.get("selectionResults");
        if (selectionResults != null) {
            return PqlParsedInfo.forSelection(selectionResults.get("columns").size(), selectionResults.get("results").size());
        }

        JsonNode aggregationResults = pqlJson.get("aggregationResults");
        int aggregates = aggregationResults.size();
        Set<List<String>> groups = new HashSet<>();
        int groupByColumns = 0;
        int pureAggregates = 0;
        for (int i = 0; i < aggregates; i++) {
            JsonNode groupByResult = aggregationResults.get(i).get("groupByResult");
            if (groupByResult != null) {
                for (int j = 0; j < groupByResult.size(); ++j) {
                    JsonNode groupJson = groupByResult.get(j).get("group");
                    List<String> group = Streams.stream(groupJson.iterator()).map(JsonNode::asText).collect(toImmutableList());
                    groups.add(group);
                    if (groupByColumns == 0) {
                        groupByColumns = group.size();
                    }
                }
            }
            else {
                pureAggregates++;
            }
        }
        assertTrue(pureAggregates == 0 || pureAggregates == aggregates, String.format("In pql response %s, got mixed aggregates %d of %d", pqlResponse, pureAggregates, aggregates));
        if (pureAggregates == 0) {
            return PqlParsedInfo.forAggregation(groupByColumns, aggregates, groups.size());
        }
        return PqlParsedInfo.forAggregation(0, pureAggregates, 1);
    }

    @DataProvider(name = "pqlResponses")
    public static Object[][] pqlResponsesProvider()
    {
        return new Object[][] {
                {
                    "SELECT count(*), sum(regionId) FROM eats_job_state GROUP BY jobState TOP 1000000",
                    "{\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10646777\",\"group\":[\"CREATED\"]},{\"value\":\"9441201\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5329962\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281666\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5225839\",\"group\":[\"OFFERED\"]},{\"value\":\"5088568\",\"group\":[\"READY\"]},{\"value\":\"5027369\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677267\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1559953\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1532913\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532891\",\"group\":[\"RELEASED\"]},{\"value\":\"531719\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252977\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243463\",\"group\":[\"CANCELED\"]},{\"value\":\"211553\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148548\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108057\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47043\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16459\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9976\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]},{\"groupByResult\":[{\"value\":\"3274799599.00000\",\"group\":[\"CREATED\"]},{\"value\":\"2926585674.00000\",\"group\":[\"ASSIGNED\"]},{\"value\":\"1645707788.00000\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"1614715326.00000\",\"group\":[\"OFFERED\"]},{\"value\":\"1608041994.00000\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"1568036720.00000\",\"group\":[\"READY\"]},{\"value\":\"1541977381.00000\",\"group\":[\"COMPLETED\"]},{\"value\":\"1190457213.00000\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"430246171.00000\",\"group\":[\"SCHEDULED\"]},{\"value\":\"422020881.00000\",\"group\":[\"RELEASED\"]},{\"value\":\"421937782.00000\",\"group\":[\"ACCEPTED\"]},{\"value\":\"147557783.00000\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"94882088.00000\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"86447788.00000\",\"group\":[\"CANCELED\"]},{\"value\":\"77505566.00000\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"53955037.00000\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"36026660.00000\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"15306755.00000\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"8811788.00000\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"5301567.00000\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"4855342.00000\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"3113490.00000\",\"group\":[\"FAILED\"]},{\"value\":\"2811789.00000\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"1053944.00000\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"sum_regionId\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":55977222,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":111954444,\"totalDocs\":55977222,\"numGroupsLimitReached\":false,\"timeUsedMs\":775,\"segmentStatistics\":[],\"traceInfo\":{}}",
                    ImmutableList.of(derived("count"), derived("sum"), jobState),
                    ImmutableList.of(2, 0, 1),
                    ImmutableList.of(derived("count"), derived("sum"), jobState),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM eats_job_state GROUP BY jobState TOP 1000000", // projecting group-by by without aggregate
                    "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55979949,\"numDocsScanned\":55979949,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10647363\",\"group\":[\"CREATED\"]},{\"value\":\"9441638\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5330203\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281905\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5226090\",\"group\":[\"OFFERED\"]},{\"value\":\"5088813\",\"group\":[\"READY\"]},{\"value\":\"5027589\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677424\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1560029\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1533006\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532980\",\"group\":[\"RELEASED\"]},{\"value\":\"531745\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252989\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243477\",\"group\":[\"CANCELED\"]},{\"value\":\"211571\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148557\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108062\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47048\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16461\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9978\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":402,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55979949}",
                    ImmutableList.of(jobState),
                    ImmutableList.of(0, -1),
                    ImmutableList.of(jobState, derived("hidden_count")),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM eats_job_state GROUP BY jobState TOP 1000000", // projecting aggregate without group-by
                    "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55979949,\"numDocsScanned\":55979949,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10647363\",\"group\":[\"CREATED\"]},{\"value\":\"9441638\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5330203\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281905\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5226090\",\"group\":[\"OFFERED\"]},{\"value\":\"5088813\",\"group\":[\"READY\"]},{\"value\":\"5027589\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677424\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1560029\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1533006\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532980\",\"group\":[\"RELEASED\"]},{\"value\":\"531745\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252989\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243477\",\"group\":[\"CANCELED\"]},{\"value\":\"211571\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148557\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108062\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47048\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16461\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9978\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":402,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55979949}",
                    ImmutableList.of(derived("count")),
                    ImmutableList.of(1, 0),
                    ImmutableList.of(derived("count"), jobState),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM eats_job_state GROUP BY jobState TOP 1000000",
                    "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55979949,\"numDocsScanned\":55979949,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"groupByResult\":[{\"value\":\"10647363\",\"group\":[\"CREATED\"]},{\"value\":\"9441638\",\"group\":[\"ASSIGNED\"]},{\"value\":\"5330203\",\"group\":[\"SUBMITTED_TO_BILLING\"]},{\"value\":\"5281905\",\"group\":[\"PICKUP_COMPLETED\"]},{\"value\":\"5226090\",\"group\":[\"OFFERED\"]},{\"value\":\"5088813\",\"group\":[\"READY\"]},{\"value\":\"5027589\",\"group\":[\"COMPLETED\"]},{\"value\":\"3677424\",\"group\":[\"SUBMITTED_TO_MANIFEST\"]},{\"value\":\"1560029\",\"group\":[\"SCHEDULED\"]},{\"value\":\"1533006\",\"group\":[\"ACCEPTED\"]},{\"value\":\"1532980\",\"group\":[\"RELEASED\"]},{\"value\":\"531745\",\"group\":[\"UNASSIGNED\"]},{\"value\":\"252989\",\"group\":[\"PREP_TIME_UPDATED\"]},{\"value\":\"243477\",\"group\":[\"CANCELED\"]},{\"value\":\"211571\",\"group\":[\"PAYMENT_PENDING\"]},{\"value\":\"148557\",\"group\":[\"PAYMENT_CONFIRMED\"]},{\"value\":\"108062\",\"group\":[\"UNFULFILLED_WARNED\"]},{\"value\":\"47048\",\"group\":[\"DELIVERY_FAILED\"]},{\"value\":\"30832\",\"group\":[\"UNFULFILLED\"]},{\"value\":\"18009\",\"group\":[\"SCHEDULE_ORDER_CREATED\"]},{\"value\":\"16461\",\"group\":[\"SCHEDULE_ORDER_ACCEPTED\"]},{\"value\":\"11086\",\"group\":[\"FAILED\"]},{\"value\":\"9978\",\"group\":[\"SCHEDULE_ORDER_OFFERED\"]},{\"value\":\"3094\",\"group\":[\"PAYMENT_FAILED\"]}],\"function\":\"count_star\",\"groupByColumns\":[\"jobState\"]}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":402,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55979949}",
                    ImmutableList.of(derived("count"), jobState),
                    ImmutableList.of(1, 0),
                    ImmutableList.of(derived("count"), jobState),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM eats_job_state",
                    "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":0,\"numDocsScanned\":55981101,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"function\":\"count_star\",\"value\":\"55981101\"}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":7,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55981101}",
                    ImmutableList.of(derived("count")),
                    ImmutableList.of(0),
                    ImmutableList.of(derived("count")),
                    Optional.empty()},
                {
                    "SELECT sum(regionId), count(*) FROM eats_job_state",
                    "{\"traceInfo\":{},\"numEntriesScannedPostFilter\":55981641,\"numDocsScanned\":55981641,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"aggregationResults\":[{\"function\":\"sum_regionId\",\"value\":\"17183585871.00000\"},{\"function\":\"count_star\",\"value\":\"55981641\"}],\"exceptions\":[],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":549,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":55981641}",
                    ImmutableList.of(derived("sum"), derived("count")),
                    ImmutableList.of(0, 1),
                    ImmutableList.of(derived("sum"), derived("count")),
                    Optional.empty()},
                {
                    "SELECT jobState, regionId FROM eats_job_state LIMIT 10",
                    "{\"selectionResults\":{\"columns\":[\"jobState\",\"regionId\"],\"results\":[[\"CREATED\",\"197\"],[\"SUBMITTED_TO_BILLING\",\"227\"],[\"ASSIGNED\",\"188\"],[\"SCHEDULED\",\"1479\"],[\"CANCELED\",\"1708\"],[\"CREATED\",\"134\"],[\"CREATED\",\"12\"],[\"OFFERED\",\"30\"],[\"COMPLETED\",\"215\"],[\"CREATED\",\"7\"]]},\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":380,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":760,\"totalDocs\":55988817,\"numGroupsLimitReached\":false,\"timeUsedMs\":2,\"segmentStatistics\":[],\"traceInfo\":{}}",
                    ImmutableList.of(jobState, regionId),
                    ImmutableList.of(0, 1),
                    ImmutableList.of(jobState, regionId),
                    Optional.empty()},
                {
                    "SELECT shoppingCartUUID, $validUntil, $validFrom, jobState, tenancy, accountUUID, vehicleViewId, $partition, clientUUID, orderJobUUID, productTypeUUID, demandJobUUID, regionId, workflowUUID, jobType, kafkaOffset, productUUID, timestamp, flowType, ts FROM eats_job_state LIMIT 10",
                    "{\"selectionResults\":{\"columns\":[\"shoppingCartUUID\",\"$validUntil\",\"$validFrom\",\"jobState\",\"tenancy\",\"accountUUID\",\"vehicleViewId\",\"$partition\",\"clientUUID\",\"orderJobUUID\",\"productTypeUUID\",\"demandJobUUID\",\"regionId\",\"workflowUUID\",\"jobType\",\"kafkaOffset\",\"productUUID\",\"timestamp\",\"flowType\",\"ts\"],\"results\":[]},\"traceInfo\":{},\"numEntriesScannedPostFilter\":0,\"numDocsScanned\":0,\"numServersResponded\":7,\"numGroupsLimitReached\":false,\"exceptions\":[{\"errorCode\":200,\"message\":\"QueryExecutionError:\\njava.lang.NullPointerException\\n\\tat java.lang.Class.forName0(Native Method\\n\\tat\"}],\"numEntriesScannedInFilter\":0,\"timeUsedMs\":3,\"segmentStatistics\":[],\"numServersQueried\":7,\"totalDocs\":0}",
                    ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.of(PinotException.class)},
                {
                    "SELECT * from eats_utilization_summarized",
                    "{\"selectionResults\": {\"columns\": [\"activeTrips\", \"numDrivers\", \"region\", \"rowtime\", \"secondsSinceEpoch\", \"utilization\", \"utilizedDrivers\", \"vehicleViewId\", \"windowEnd\", \"windowStart\"], \"results\": [[\"0\", \"0\", \"foobar\", null, \"4588780800\", \"-∞\", \"0\", \"20017545\", \"4588780740000\", \"4588780725000\"], [\"8699\", \"11452\", \"doobar\", null, \"4588780800\", \"0.730701685\", \"8368\", \"0\", \"4588780740000\", \"4588780725000\"], [\"0\", \"14\", \"zoobar\", null, \"4588780800\", \"0.5\", \"7\", \"20014789\", \"4588780740000\", \"4588780725000\"], [\"0\", \"23\", \"moobar\", null, \"4588780800\", \"0.4336180091\", \"10\", \"20009983\", \"4588780740000\", \"4588780725000\"], [\"0\", \"840\", \"koobar\", null, \"4588780800\", \"0.6597985029\", \"554\", \"20006875\", \"4588780740000\", \"4588780725000\"], [\"0\", \"0\", \"loobar\", null, \"4588780800\", \"-∞\", \"0\", \"20006291\", \"4588780740000\", \"4588780725000\"], [\"15\", \"1832\", \"monkeybar\", null, \"4588780800\", \"0.8792306185\", \"1610\", \"20004007\", \"4588780740000\", \"4588780725000\"], [\"0\", \"0\", \"donkeybar\", null, \"4588780800\", \"-∞\", \"0\", \"0\", \"4588780740000\", \"4588780725000\"], [\"1\", \"7\", \"horseybar\", null, \"4588780800\", \"0.2857142985\", \"2\", \"20016753\", \"4588780740000\", \"4588780725000\"], [\"0\", \"130\", \"ginbar\", null, \"4588780800\", \"0.8052611947\", \"105\", \"10000942\", \"4588780740000\", \"4588780725000\"]]}, \"exceptions\": [], \"numServersQueried\": 4, \"numServersResponded\": 4, \"numSegmentsQueried\": 24, \"numSegmentsProcessed\": 24, \"numSegmentsMatched\": 24, \"numDocsScanned\": 240, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 240, \"numGroupsLimitReached\": false, \"totalDocs\": 1000, \"timeUsedMs\": 6, \"segmentStatistics\": [], \"traceInfo\": {}}",
                    ImmutableList.of(bigint("activeTrips"), bigint("numDrivers"), varchar("region"), bigint("rowtime"), secondsSinceEpoch, fraction("utilization"), bigint("utilizedDrivers"), bigint("vehicleViewId"), bigint("windowEnd"), bigint("windowStart")),
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                    ImmutableList.of(bigint("activeTrips"), bigint("numDrivers"), varchar("region"), bigint("rowtime"), secondsSinceEpoch, fraction("utilization"), bigint("utilizedDrivers"), bigint("vehicleViewId"), bigint("windowEnd"), bigint("windowStart")),
                    Optional.empty()},
                {"SELECT tag_value FROM eats_job_state LIMIT 10",
                        "{\"selectionResults\":{\"columns\":[\"tag_value\"],\"results\":[[[\"tag1\"]],[[\"tag1\", \"tag2\"]],[[\"tag2\", \"tag3\"]],[[\"null\"]],[[\"tag4\", \"tag2\", \"tag1\", \"tag5\"]],[[\"tag2\"]],[[\"null\"]],[[\"null\"]],[[\"tag1\", \"tag6\"]],[[\"null\"]]]},\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":380,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":760,\"totalDocs\":55988817,\"numGroupsLimitReached\":false,\"timeUsedMs\":2,\"segmentStatistics\":[],\"traceInfo\":{}}",
                        ImmutableList.of(array(VARCHAR, "tag_value")),
                        ImmutableList.of(0),
                        ImmutableList.of(array(VARCHAR, "tag_value")),
                        Optional.empty()},
                {"SELECT num_values FROM eats_job_state LIMIT 10",
                        "{\"selectionResults\":{\"columns\":[\"tag_value\"],\"results\":[[[\"123\"]],[[\"456\", \"567\"]],[[\"2345\", \"8907\"]],[[\"0\"]],[[\"123\", \"1234\", \"987\", \"1678\"]],[[\"98\"]],[[\"0\"]],[[\"0\"]],[[\"1\", \"0\"]],[[\"0\"]]]},\"exceptions\":[],\"numServersQueried\":7,\"numServersResponded\":7,\"numDocsScanned\":380,\"numEntriesScannedInFilter\":0,\"numEntriesScannedPostFilter\":760,\"totalDocs\":55988817,\"numGroupsLimitReached\":false,\"timeUsedMs\":2,\"segmentStatistics\":[],\"traceInfo\":{}}",
                        ImmutableList.of(array(BIGINT, "num_values")),
                        ImmutableList.of(0),
                        ImmutableList.of(array(BIGINT, "num_values")),
                        Optional.empty()},
        };
    }

    @Test
    public void testPinotBrokerRequest()
    {
        PinotQueryGenerator.GeneratedPinotQuery generatedPinotQuery = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                "SELECT * FROM myTable",
                PinotQueryGenerator.PinotQueryFormat.PQL,
                ImmutableList.of(),
                0,
                false,
                false);

        PinotBrokerPageSourcePql pageSource = new PinotBrokerPageSourcePql(
                pinotConfig,
                new TestingConnectorSession(ImmutableList.of(
                        booleanProperty(
                                "mark_data_fetch_exceptions_as_retriable",
                                "Retry Pinot query on data fetch exceptions",
                                pinotConfig.isMarkDataFetchExceptionsAsRetriable(),
                                false))),
                generatedPinotQuery,
                ImmutableList.of(),
                ImmutableList.of(),
                new MockPinotClusterInfoFetcher(pinotConfig),
                objectMapper);
        assertEquals(pageSource.getRequestPayload(generatedPinotQuery), "{\"pql\":\"SELECT * FROM myTable\"}");

        generatedPinotQuery = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                "SELECT * FROM myTable WHERE jsonStr = '\"{\"abc\" : \"def\"}\"'",
                PinotQueryGenerator.PinotQueryFormat.PQL,
                ImmutableList.of(),
                0,
                false,
                false);
        assertEquals(pageSource.getRequestPayload(generatedPinotQuery), "{\"pql\":\"SELECT * FROM myTable WHERE jsonStr = '\\\"{\\\"abc\\\" : \\\"def\\\"}\\\"'\"}");
    }

    @Test(dataProvider = "pqlResponses")
    public void testPopulateFromPql(
            String pql,
            String pqlResponse,
            List<PinotColumnHandle> actualHandles,
            List<Integer> expectedColumnIndices,
            List<PinotColumnHandle> expectedColumnHandles,
            Optional<Class<? extends PrestoException>> expectedError)
            throws IOException
    {
        PqlParsedInfo pqlParsedInfo = getBasicInfoFromPql(pqlResponse);
        PinotQueryGenerator.GeneratedPinotQuery generatedPql = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                pql,
                PinotQueryGenerator.PinotQueryFormat.PQL,
                expectedColumnIndices,
                pqlParsedInfo.groupByColumns,
                false,
                false);
        PinotBrokerPageSourcePql pageSource = new PinotBrokerPageSourcePql(
                pinotConfig,
                new TestingConnectorSession(ImmutableList.of(
                    booleanProperty(
                        "mark_data_fetch_exceptions_as_retriable",
                        "Retry Pinot query on data fetch exceptions",
                        pinotConfig.isMarkDataFetchExceptionsAsRetriable(),
                        false))),
                generatedPql,
                actualHandles,
                expectedColumnHandles,
                new MockPinotClusterInfoFetcher(pinotConfig),
                objectMapper);
        PinotBrokerPageSourceBase.BlockAndTypeBuilder blockAndTypeBuilder = pageSource.buildBlockAndTypeBuilder(actualHandles, generatedPql);

        validateExpectedColumnIndices(expectedColumnIndices, expectedColumnHandles);
        List<BlockBuilder> columnBlockBuilders = blockAndTypeBuilder.getColumnBlockBuilders();
        List<Type> columnTypes = blockAndTypeBuilder.getColumnTypes();

        assertEquals(columnTypes.size(), columnBlockBuilders.size());

        int numNonNullTypes = 0;
        for (int i = 0; i < columnTypes.size(); i++) {
            Type type = columnTypes.get(i);
            BlockBuilder builder = columnBlockBuilders.get(i);
            assertEquals(type == null, builder == null);
            if (type != null) {
                numNonNullTypes++;
            }
        }
        assertEquals(numNonNullTypes, actualHandles.size());

        Optional<? extends PrestoException> thrown = Optional.empty();
        int rows = -1;
        try {
            rows = pageSource.populateFromQueryResults(
                    generatedPql,
                    columnBlockBuilders,
                    columnTypes,
                    pqlResponse);
        }
        catch (PrestoException e) {
            thrown = Optional.of(e);
        }

        Optional<? extends Class<? extends PrestoException>> thrownType = thrown.map(e -> e.getClass());
        Optional<String> errorString = thrown.map(e -> Throwables.getStackTraceAsString(e));
        assertEquals(thrownType, expectedError, String.format("Expected error %s, but got error of type %s: %s", expectedError, thrownType, errorString));
        if (!expectedError.isPresent()) {
            assertEquals(expectedColumnIndices.size(), pqlParsedInfo.columns);
            assertEquals(rows, pqlParsedInfo.rows);
        }
    }

    private void validateExpectedColumnIndices(List<Integer> expectedColumnIndices, List<PinotColumnHandle> expectedColumnHandles)
    {
        int numValid = 0;
        Set<Integer> uniqueIndices = new HashSet<>();
        for (int expectedColumnIndex : expectedColumnIndices) {
            assertTrue(expectedColumnIndex == -1 || expectedColumnIndex >= 0 && expectedColumnIndex < expectedColumnHandles.size());
            if (expectedColumnIndex >= 0) {
                numValid++;
                uniqueIndices.add(expectedColumnIndex);
            }
        }
        assertEquals(numValid, uniqueIndices.size());
    }
}
