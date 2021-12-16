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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static org.testng.Assert.assertEquals;

public class TestPinotBrokerPageSourceSql
        extends TestPinotQueryBase
{
    private static PinotTableHandle pinotTable = new PinotTableHandle("connId", "schema", "tbl");
    private final ObjectMapper objectMapper = new JsonObjectMapperProvider().get();
    private static PinotColumnHandle groupCountry = new PinotColumnHandle("group_country", VARCHAR, PinotColumnHandle.PinotColumnType.REGULAR);
    private static PinotColumnHandle groupCity = new PinotColumnHandle("group_city", VARCHAR, PinotColumnHandle.PinotColumnType.REGULAR);

    private static class SqlParsedInfo
    {
        final int columns;
        final int rows;

        public SqlParsedInfo(int columns, int rows)
        {
            this.columns = columns;
            this.rows = rows;
        }
    }

    SqlParsedInfo getBasicInfoFromSql(String sqlResponse)
            throws IOException
    {
        int numRows = 0;
        int numColumns = 0;
        JsonNode sqlJson = objectMapper.readTree(sqlResponse);
        JsonNode resultTable = sqlJson.get("resultTable");
        if (resultTable != null) {
            JsonNode rows = resultTable.get("rows");
            if (rows != null) {
                numRows = rows.size();
            }
            JsonNode dataSchema = resultTable.get("dataSchema");
            if (dataSchema != null) {
                JsonNode columnNames = dataSchema.get("columnNames");
                if (columnNames != null) {
                    numColumns = columnNames.size();
                }
            }
        }
        return new SqlParsedInfo(numColumns, numRows);
    }

    @DataProvider(name = "sqlResponses")
    public static Object[][] sqlResponsesProvider()
    {
        return new Object[][] {
                {
                    "SELECT group_country, count(*), sum(rsvp_count) FROM meetupRsvp GROUP BY group_country LIMIT 1000000",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"STRING\", \"LONG\", \"DOUBLE\"], \"columnNames\": [\"group_country\", \"count(*)\", \"sum(rsvp_count)\"] }, \"rows\": [ [\"ch\", 11, 11.0], [\"kr\", 32, 32.0], [\"cl\", 1, 1.0], [\"gr\", 2, 2.0], [\"co\", 2, 2.0], [\"cz\", 4, 4.0], [\"ph\", 1, 1.0], [\"pl\", 16, 16.0], [\"li\", 1, 1.0], [\"tr\", 14, 14.0], [\"de\", 45, 45.0], [\"tw\", 3, 3.0], [\"hk\", 14, 14.0], [\"pt\", 13, 13.0], [\"dk\", 5, 5.0], [\"lu\", 2, 2.0], [\"ua\", 1, 1.0], [\"id\", 5, 5.0], [\"ie\", 13, 13.0], [\"us\", 205, 205.0], [\"eg\", 1, 1.0], [\"il\", 14, 14.0], [\"ae\", 4, 4.0], [\"in\", 32, 32.0], [\"za\", 6, 6.0], [\"it\", 4, 4.0], [\"mx\", 1, 1.0], [\"my\", 1, 1.0], [\"es\", 79, 79.0], [\"ar\", 5, 5.0], [\"at\", 14, 14.0], [\"au\", 119, 119.0], [\"ng\", 1, 1.0], [\"ro\", 3, 3.0], [\"nl\", 24, 24.0], [\"no\", 1, 1.0], [\"be\", 2, 2.0], [\"jp\", 32, 32.0], [\"fr\", 35, 35.0], [\"nz\", 12, 12.0], [\"sa\", 1, 1.0], [\"br\", 20, 20.0], [\"se\", 24, 24.0], [\"sg\", 49, 49.0], [\"sk\", 2, 2.0], [\"ke\", 7, 7.0], [\"gb\", 159, 159.0], [\"ca\", 11, 11.0] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 1053, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 2106, \"numGroupsLimitReached\": false, \"totalDocs\": 1053, \"timeUsedMs\": 7, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592909715708 }",
                    ImmutableList.of(groupCountry, derived("count"), derived("sum")),
                    ImmutableList.of(0, 1, 2),
                    Optional.empty()},
                {
                    "SELECT group_country, count(*) FROM meetupRsvp GROUP BY group_country LIMIT 1000000",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"STRING\", \"LONG\"], \"columnNames\": [\"group_country\", \"count(*)\"] }, \"rows\": [ [\"eg\", 1], [\"il\", 12], [\"ae\", 4], [\"in\", 31], [\"ch\", 10], [\"kr\", 30], [\"za\", 6], [\"cl\", 1], [\"it\", 4], [\"gr\", 1], [\"my\", 1], [\"co\", 1], [\"es\", 75], [\"ar\", 5], [\"at\", 12], [\"au\", 108], [\"cz\", 4], [\"ph\", 1], [\"ng\", 1], [\"pl\", 15], [\"ro\", 3], [\"li\", 1], [\"nl\", 20], [\"tr\", 14], [\"de\", 35], [\"no\", 1], [\"hk\", 14], [\"tw\", 2], [\"be\", 2], [\"pt\", 12], [\"jp\", 28], [\"dk\", 4], [\"lu\", 2], [\"nz\", 11], [\"fr\", 28], [\"ua\", 1], [\"sa\", 1], [\"br\", 18], [\"se\", 24], [\"sg\", 43], [\"sk\", 2], [\"ke\", 7], [\"gb\", 146], [\"id\", 5], [\"ie\", 13], [\"us\", 171], [\"ca\", 9] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 940, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 940, \"numGroupsLimitReached\": false, \"totalDocs\": 940, \"timeUsedMs\": 4, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592909613611 }",
                    ImmutableList.of(groupCountry, derived("count")),
                    ImmutableList.of(0, 1),
                    Optional.empty()},
                {
                    "SELECT count(*), group_country FROM meetupRsvp GROUP BY group_country LIMIT 1000000",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnNames\": [\"count(*)\", \"group_country\"], \"columnDataTypes\": [\"LONG\", \"STRING\"] }, \"rows\": [ [1, \"gh\"], [1, \"gn\"], [48, \"gr\"], [15, \"gt\"], [39, \"pa\"], [203, \"pe\"], [7, \"pf\"], [34, \"ph\"], [30, \"pk\"], [379, \"pl\"], [2, \"ps\"], [896, \"hk\"], [275, \"pt\"], [4, \"hn\"], [8, \"py\"], [5, \"hr\"], [68, \"hu\"], [10, \"qa\"], [279, \"id\"], [553, \"ie\"], [254, \"il\"], [355, \"ae\"], [2618, \"in\"], [211, \"za\"], [64, \"iq\"], [270, \"it\"], [6, \"al\"], [5, \"am\"], [8, \"ao\"], [315, \"ar\"], [330, \"at\"], [1, \"zm\"], [3282, \"au\"], [3, \"az\"], [55, \"ro\"], [2, \"ba\"], [84, \"rs\"], [34, \"bd\"], [87, \"ru\"], [183, \"be\"], [13, \"bg\"], [1472, \"jp\"], [26, \"bh\"], [3, \"bi\"], [64, \"bo\"], [149, \"sa\"], [1066, \"br\"], [6, \"sd\"], [527, \"se\"], [936, \"sg\"], [10, \"si\"], [11, \"sk\"], [178, \"ke\"], [27, \"sn\"], [2958, \"ca\"], [6, \"cd\"], [5, \"sv\"], [1, \"cg\"], [705, \"ch\"], [40, \"ci\"], [2036, \"kr\"], [174, \"cl\"], [3, \"cm\"], [56, \"cn\"], [246, \"co\"], [3, \"kw\"], [29, \"cr\"], [150, \"th\"], [11, \"lb\"], [3, \"cy\"], [155, \"cz\"], [12, \"tm\"], [6, \"tn\"], [3, \"li\"], [329, \"tr\"], [74, \"lk\"], [2882, \"de\"], [124, \"tw\"], [6, \"lr\"], [1, \"tz\"], [121, \"dk\"], [47, \"lt\"], [49, \"lu\"], [15, \"lv\"], [4, \"do\"], [58, \"ua\"], [1, \"ug\"], [27, \"ma\"], [5, \"dz\"], [12, \"ec\"], [28498, \"us\"], [7, \"mk\"], [5, \"ml\"], [16, \"ee\"], [6, \"mm\"], [2, \"mn\"], [116, \"eg\"], [5, \"mo\"], [71, \"uy\"], [253, \"mx\"], [100, \"my\"], [3249, \"es\"], [1, \"et\"], [2, \"ve\"], [36, \"vn\"], [204, \"ng\"], [21, \"ni\"], [946, \"nl\"], [93, \"no\"], [12, \"np\"], [22, \"fi\"], [2223, \"fr\"], [767, \"nz\"], [14, \"ga\"], [4524, \"gb\"], [14, \"ge\"], [1, \"om\"] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 5, \"numSegmentsProcessed\": 4, \"numSegmentsMatched\": 4, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 67077, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 67077, \"numGroupsLimitReached\": false, \"totalDocs\": 67077, \"timeUsedMs\": 10, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 9223372036854775807 }",
                    ImmutableList.of(derived("count"), groupCountry),
                    ImmutableList.of(0, 1),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM meetupRsvp GROUP BY group_country LIMIT 1000000",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnNames\": [\"count(*)\"], \"columnDataTypes\": [\"LONG\"] }, \"rows\": [ [1], [1], [48], [15], [39], [203], [7], [34], [30], [379], [2], [896], [275], [4], [8], [5], [68], [10], [279], [553], [254], [355], [2618], [211], [64], [270], [6], [5], [8], [315], [330], [1], [3282], [3], [55], [2], [84], [34], [87], [183], [13], [1472], [26], [3], [64], [149], [1066], [6], [527], [936], [10], [11], [178], [27], [2958], [6], [5], [1], [705], [40], [2036], [174], [3], [56], [246], [3], [29], [150], [11], [3], [155], [12], [6], [3], [329], [74], [2882], [124], [6], [1], [121], [47], [49], [15], [4], [58], [1], [27], [5], [12], [28498], [7], [5], [16], [6], [2], [116], [5], [71], [253], [100], [3249], [1], [2], [36], [204], [21], [946], [93], [12], [22], [2223], [767], [14], [4524], [14], [1] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 5, \"numSegmentsProcessed\": 4, \"numSegmentsMatched\": 4, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 67077, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 67077, \"numGroupsLimitReached\": false, \"totalDocs\": 67077, \"timeUsedMs\": 31, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 9223372036854775807 }",
                    ImmutableList.of(derived("count")),
                    ImmutableList.of(0),
                    Optional.empty()},
                {
                    "SELECT distinct group_country FROM meetupRsvp  LIMIT 1000000",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnNames\": [\"group_country\"], \"columnDataTypes\": [\"STRING\"] }, \"rows\": [ [\"np\"], [\"az\"], [\"ar\"], [\"fr\"], [\"cd\"], [\"kr\"], [\"zm\"], [\"gn\"], [\"ro\"], [\"ao\"], [\"ie\"], [\"et\"], [\"bh\"], [\"gh\"], [\"ke\"], [\"lk\"], [\"ph\"], [\"cl\"], [\"vn\"], [\"ma\"], [\"mo\"], [\"cz\"], [\"ba\"], [\"nz\"], [\"ru\"], [\"at\"], [\"be\"], [\"in\"], [\"my\"], [\"ga\"], [\"cr\"], [\"us\"], [\"ug\"], [\"cy\"], [\"li\"], [\"mm\"], [\"do\"], [\"cn\"], [\"es\"], [\"pt\"], [\"dk\"], [\"fi\"], [\"si\"], [\"no\"], [\"am\"], [\"sk\"], [\"cg\"], [\"ci\"], [\"lu\"], [\"py\"], [\"mk\"], [\"gt\"], [\"qa\"], [\"th\"], [\"ec\"], [\"pk\"], [\"bg\"], [\"bo\"], [\"om\"], [\"nl\"], [\"sd\"], [\"rs\"], [\"il\"], [\"za\"], [\"ua\"], [\"hk\"], [\"lb\"], [\"tm\"], [\"gr\"], [\"it\"], [\"pf\"], [\"uy\"], [\"ee\"], [\"mx\"], [\"jp\"], [\"sg\"], [\"sa\"], [\"ng\"], [\"iq\"], [\"pa\"], [\"cm\"], [\"mn\"], [\"tr\"], [\"lr\"], [\"hn\"], [\"hu\"], [\"eg\"], [\"id\"], [\"gb\"], [\"tz\"], [\"sn\"], [\"al\"], [\"sv\"], [\"dz\"], [\"pl\"], [\"lt\"], [\"hr\"], [\"tn\"], [\"ve\"], [\"bi\"], [\"bd\"], [\"se\"], [\"au\"], [\"ae\"], [\"kw\"], [\"ps\"], [\"pe\"], [\"lv\"], [\"de\"], [\"tw\"], [\"co\"], [\"ml\"], [\"ca\"], [\"ge\"], [\"ch\"], [\"br\"], [\"ni\"] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 5, \"numSegmentsProcessed\": 4, \"numSegmentsMatched\": 4, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 67077, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 67077, \"numGroupsLimitReached\": false, \"totalDocs\": 67077, \"timeUsedMs\": 44, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 9223372036854775807 }",
                    ImmutableList.of(groupCountry),
                    ImmutableList.of(0),
                    Optional.empty()},
                {
                    "SELECT count(*) FROM meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"LONG\"], \"columnNames\": [\"count(*)\"] }, \"rows\": [ [1090] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 1090, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 0, \"numGroupsLimitReached\": false, \"totalDocs\": 1090, \"timeUsedMs\": 10, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592909751127 }",
                    ImmutableList.of(derived("count")),
                    ImmutableList.of(0),
                    Optional.empty()},
                {
                    "SELECT sum(rsvp_count), count(*) FROM meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"DOUBLE\", \"LONG\"], \"columnNames\": [\"sum(rsvp_count)\", \"count(*)\"] }, \"rows\": [ [1138.0, 1138] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 1138, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 1138, \"numGroupsLimitReached\": false, \"totalDocs\": 1138, \"timeUsedMs\": 12, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592909798452 }",
                    ImmutableList.of(derived("sum"), derived("count")),
                    ImmutableList.of(0, 1),
                    Optional.empty()},
                {
                    "SELECT group_country, group_city FROM meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"STRING\", \"STRING\"], \"columnNames\": [\"group_country\", \"group_city\"] }, \"rows\": [ [\"nl\", \"Amsterdam\"], [\"us\", \"Edison\"], [\"au\", \"Sydney\"], [\"gb\", \"Loxley\"], [\"kr\", \"Seoul\"], [\"es\", \"Barcelona\"], [\"ie\", \"Dublin\"], [\"ar\", \"Buenos Aires\"], [\"es\", \"Madrid\"], [\"kr\", \"Seoul\"] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 10, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 20, \"numGroupsLimitReached\": false, \"totalDocs\": 1210, \"timeUsedMs\": 13, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592909849974 }",
                    ImmutableList.of(groupCountry, groupCity),
                    ImmutableList.of(0, 1),
                    Optional.empty()},
                {
                    "SELECT max(group_country) FROM meetupRsvp",
                    "{ \"exceptions\": [ { \"errorCode\": 200, \"message\": \"QueryExecutionError:\\njava.lang.NumberFormatException: For input string: \\\"nl\\\"\\n\\tat sun.misc.FloatingDecimal.readJavaFormatString(FloatingDecimal.java:2043)\\n\\tat sun.misc.FloatingDecimal.parseDouble(FloatingDecimal.java:110)\\n\\tat java.lang.Double.parseDouble(Double.java:538)\\n\\tat org.apache.pinot.core.realtime.impl.dictionary.StringOnHeapMutableDictionary.getDoubleValue(StringOnHeapMutableDictionary.java:138)\\n\\tat org.apache.pinot.core.segment.index.readers.BaseDictionary.readDoubleValues(BaseDictionary.java:55)\\n\\tat org.apache.pinot.core.common.DataFetcher.fetchDoubleValues(DataFetcher.java:163)\\n\\tat org.apache.pinot.core.common.DataBlockCache.getDoubleValuesForSVColumn(DataBlockCache.java:166)\\n\\tat org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet.getDoubleValuesSV(ProjectionBlockValSet.java:85)\\n\\tat org.apache.pinot.core.query.aggregation.function.MaxAggregationFunction.aggregate(MaxAggregationFunction.java:62)\\n\\tat org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor.aggregate(DefaultAggregationExecutor.java:47)\\n\\tat org.apache.pinot.core.operator.query.AggregationOperator.getNextBlock(AggregationOperator.java:65)\\n\\tat org.apache.pinot.core.operator.query.AggregationOperator.getNextBlock(AggregationOperator.java:35)\\n\\tat org.apache.pinot.core.operator.BaseOperator.nextBlock(BaseOperator.java:49)\\n\\tat org.apache.pinot.core.operator.CombineOperator$1.runJob(CombineOperator.java:105)\" }], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 1376, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 1376, \"numGroupsLimitReached\": false, \"totalDocs\": 1376, \"timeUsedMs\": 26, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592910013309 }",
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.of(PinotException.class)},
                {
                    "select * from meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"STRING\", \"STRING\", \"LONG\", \"STRING\", \"STRING\", \"LONG\", \"STRING\", \"LONG\", \"INT\", \"STRING\"], \"columnNames\": [\"event_id\", \"event_name\", \"event_time\", \"group_city\", \"group_country\", \"group_id\", \"group_name\", \"mtime\", \"rsvp_count\", \"venue_name\"] }, \"rows\": [ [\"271449847\", \"☀️Boat trip for expats ☀️Summer is back on Saturday 27☀️\", 1593253800000, \"Amsterdam\", \"nl\", 22032818, \"AIC: Amsterdam International Community for expats\", 1592908606814, 1, \"Boat Trip-Pick-up point \"], [\"271468544\", \"How To Start A Career In Privacy & Data Protection\", 1593266400000, \"Edison\", \"us\", 32769803, \"Cybersecurity Careers\", 1592908608026, 1, \"Online event\"], [\"270836483\", \"Episode #2 Power Platform Pub Quiz - Virtual #PPPQ\", 1594800000000, \"Sydney\", \"au\", 33746135, \"Power Platform Pub Quiz #PPPQ\", 1592908588000, 1, \"Online event\"], [\"271472391\", \"Ben Nevis  , Three Sisters , Aonach Eagach (hiking, scrambling and bbq ing)\uD83E\uDD17\", 1594998000000, \"Loxley\", \"gb\", 33286887, \"⭐  \uD83D\uDC10Mountain Pervs\uD83D\uDC12  ⭐\", 1592908609397, 1, \"Online event\"], [\"mksgjrybcjbjc\", \"홍대 펍파티! HONGDAE International Pub Party! Meet local and foreign friends!\", 1593165600000, \"Seoul\", \"kr\", 26806362, \"HONGDAE LANGUAGE EXCHANGE CAFE & PUB: GSM TERRACE HONGDAE\", 1592908609962, 1, \"HONGDAE GSM Terrace\"], [\"271336915\", \"\uD83C\uDF34 Un día en Cadaqués / A day in Cadaqués \uD83D\uDC1F 20€\", 1593324000000, \"Barcelona\", \"es\", 15442262, \"Barcelona Language Exchange\", 1592908610993, 1, \"Barcelona\"], [\"271312704\", \"Create your startup: The Checklist (online Event)\", 1593012600000, \"Dublin\", \"ie\", 33262613, \"Startup advice: Build My Unicorn (Dublin - Online)\", 1592908611199, 1, \"Online event\"], [\"271351670\", \"[5] Planeamiento Ágil (Desarrollo de Software Ágil en 10Pines)\", 1593034200000, \"Buenos Aires\", \"ar\", 2811362, \"Ágiles Argentina\", 1592908611403, 1, \"Online event\"], [\"270213022\", \"MARATÓN VIRTUAL POWER PLATFORM\", 1594623600000, \"Madrid\", \"es\", 19418102, \"Power BI Spain Users Group\", 1592908611966, 1, \"Online event\"], [\"mksgjrybcjbjc\", \"홍대 펍파티! HONGDAE International Pub Party! Meet local and foreign friends!\", 1593165600000, \"Seoul\", \"kr\", 26806362, \"HONGDAE LANGUAGE EXCHANGE CAFE & PUB: GSM TERRACE HONGDAE\", 1592908612111, 1, \"HONGDAE GSM Terrace\"] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 10, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 100, \"numGroupsLimitReached\": false, \"totalDocs\": 1425, \"timeUsedMs\": 10, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592910063563 }",
                    ImmutableList.of(varchar("event_id"), varchar("event_name"), bigint("event_time"), groupCity, groupCountry, bigint("group_id"), varchar("group_name"), bigint("mtime"), integer("rsvp_count"), varchar("venue_name")),
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                    Optional.empty()},
                {
                    "select event_tags from meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"STRING\"], \"columnNames\": [\"event_tags\"] }, \"rows\": [ [[\"social\"]], [[\"social\", \"corporate\"]], [[\"cinema\", \"corporate\"]], [[\"null\"]], [[\"outdoor\", \"music\", \"large_gathering\", \"expensive\"]], [[\"null\"]] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 10, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 100, \"numGroupsLimitReached\": false, \"totalDocs\": 1425, \"timeUsedMs\": 10, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592910063563 }",
                    ImmutableList.of(array(VARCHAR, "event_tags")),
                    ImmutableList.of(0),
                    Optional.empty()},
                {
                    "select tag_nums from meetupRsvp",
                    "{ \"resultTable\": { \"dataSchema\": { \"columnDataTypes\": [\"BIGINT\"], \"columnNames\": [\"tag_nums\"] }, \"rows\": [ [[\"1\"]], [[\"1\", \"2\"]], [[\"3\", \"2\"]], [[\"0\"]], [[\"4\", \"5\", \"6\", \"7\"]], [[\"0\"]] ] }, \"exceptions\": [], \"numServersQueried\": 1, \"numServersResponded\": 1, \"numSegmentsQueried\": 1, \"numSegmentsProcessed\": 1, \"numSegmentsMatched\": 1, \"numConsumingSegmentsQueried\": 1, \"numDocsScanned\": 10, \"numEntriesScannedInFilter\": 0, \"numEntriesScannedPostFilter\": 100, \"numGroupsLimitReached\": false, \"totalDocs\": 1425, \"timeUsedMs\": 10, \"segmentStatistics\": [], \"traceInfo\": {}, \"minConsumingFreshnessTimeMs\": 1592910063563 }",
                    ImmutableList.of(array(BIGINT, "tag_nums")),
                    ImmutableList.of(0),
                    Optional.empty()}
        };
    }
    @Test
    public void testPinotBrokerRequest()
    {
        PinotQueryGenerator.GeneratedPinotQuery generatedPinotQuery = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                "SELECT * FROM myTable",
                PinotQueryGenerator.PinotQueryFormat.SQL,
                ImmutableList.of(),
                0,
                false,
                false);

        PinotBrokerPageSourceSql pageSource = new PinotBrokerPageSourceSql(
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
        assertEquals(pageSource.getRequestPayload(generatedPinotQuery), "{\"sql\":\"SELECT * FROM myTable\"}");

        generatedPinotQuery = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                "SELECT * FROM myTable WHERE jsonStr = '\"{\"abc\" : \"def\"}\"'",
                PinotQueryGenerator.PinotQueryFormat.SQL,
                ImmutableList.of(),
                0,
                false,
                false);
        assertEquals(pageSource.getRequestPayload(generatedPinotQuery), "{\"sql\":\"SELECT * FROM myTable WHERE jsonStr = '\\\"{\\\"abc\\\" : \\\"def\\\"}\\\"'\"}");
    }

    @Test(dataProvider = "sqlResponses")
    public void testPopulateFromSql(
            String sql,
            String sqlResponse,
            List<PinotColumnHandle> actualHandles,
            List<Integer> expectedColumnIndices,
            Optional<Class<? extends PrestoException>> expectedError)
            throws IOException
    {
        SqlParsedInfo sqlParsedInfo = getBasicInfoFromSql(sqlResponse);
        PinotQueryGenerator.GeneratedPinotQuery generatedSql = new PinotQueryGenerator.GeneratedPinotQuery(
                pinotTable.getTableName(),
                sql,
                PinotQueryGenerator.PinotQueryFormat.SQL,
                expectedColumnIndices,
                0,
                false,
                false);
        PinotBrokerPageSourceSql pageSource = new PinotBrokerPageSourceSql(
                pinotConfig,
                new TestingConnectorSession(ImmutableList.of()),
                generatedSql,
                actualHandles,
                actualHandles,
                new MockPinotClusterInfoFetcher(pinotConfig),
                objectMapper);
        PinotBrokerPageSourceBase.BlockAndTypeBuilder blockAndTypeBuilder = pageSource.buildBlockAndTypeBuilder(actualHandles, generatedSql);
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
                    generatedSql,
                    columnBlockBuilders,
                    columnTypes,
                    sqlResponse);
        }
        catch (PrestoException e) {
            thrown = Optional.of(e);
        }

        Optional<? extends Class<? extends PrestoException>> thrownType = thrown.map(e -> e.getClass());
        Optional<String> errorString = thrown.map(e -> Throwables.getStackTraceAsString(e));
        assertEquals(thrownType, expectedError, String.format("Expected error %s, but got error of type %s: %s", expectedError, thrownType, errorString));
        if (!expectedError.isPresent()) {
            assertEquals(actualHandles.size(), sqlParsedInfo.columns);
            assertEquals(rows, sqlParsedInfo.rows);
        }
    }
}
