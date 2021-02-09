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
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static com.facebook.presto.kafka.util.EmbeddedKafka.createEmbeddedKafka;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.util.Objects.requireNonNull;

@Test
public class TestKafkaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final EmbeddedKafka embeddedKafka;

    public TestKafkaIntegrationSmokeTest()
            throws Exception
    {
        this(createEmbeddedKafka());
    }

    public TestKafkaIntegrationSmokeTest(EmbeddedKafka embeddedKafka)
    {
        super(() -> createKafkaQueryRunner(embeddedKafka, ORDERS));
        this.embeddedKafka = embeddedKafka;
    }

    @Test(dataProvider = "roundTripAllFormatsDataProvider")
    public void testRoundTripAllFormats(RoundTripTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTableName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES " + testCase.getRowValues(), testCase.getNumRows());
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                " WHERE f_bigint > 1",
                "VALUES " + testCase.getRowValues());
    }

    @DataProvider
    public final Object[][] roundTripAllFormatsDataProvider()
    {
        return roundTripAllFormatsData().stream()
                .collect(toDataProvider());
    }

    private List<RoundTripTestCase> roundTripAllFormatsData()
    {
        return ImmutableList.<RoundTripTestCase>builder()
                .add(new RoundTripTestCase(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_json",
                        ImmutableList.of("f_bigint", "f_int", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123748, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .build();
    }

    private static final class RoundTripTestCase
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<List<Object>> rowValues;
        private final int numRows;

        public RoundTripTestCase(String tableName, List<String> fieldNames, List<List<Object>> rowValues)
        {
            for (List<Object> row : rowValues) {
                checkArgument(fieldNames.size() == row.size(), "sizes of fieldNames and rowValues are not equal");
            }
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.rowValues = ImmutableList.copyOf(rowValues);
            this.numRows = this.rowValues.size();
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getFieldNames()
        {
            return String.join(", ", fieldNames);
        }

        public String getRowValues()
        {
            String[] rows = new String[numRows];
            for (int i = 0; i < numRows; i++) {
                rows[i] = rowValues.get(i).stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
            }
            return String.join(", ", rows);
        }

        public int getNumRows()
        {
            return numRows;
        }

        @Override
        public String toString()
        {
            return tableName; // for test case label in IDE
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        embeddedKafka.close();
    }
}
