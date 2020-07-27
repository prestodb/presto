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

    @Test(dataProvider = "testRoundTripAllFormatsDataProvider")
    public void testRoundTripAllFormats(RoundTripTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTableName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES (" + testCase.getFieldValues() + "), (" + testCase.getFieldValues() + ")", 2);
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                " WHERE " + testCase.getFieldName("f_bigint") + " = " + testCase.getFieldValue("f_bigint"),
                "VALUES (" + testCase.getFieldValues() + "), (" + testCase.getFieldValues() + ")");
    }

    @DataProvider(name = "testRoundTripAllFormatsDataProvider")
    public final Object[][] testRoundTripAllFormatsDataProvider()
    {
        return testRoundTripAllFormatsData().stream()
                .collect(toDataProvider());
    }

    private List<RoundTripTestCase> testRoundTripAllFormatsData()
    {
        return ImmutableList.<RoundTripTestCase>builder()
                .add(new RoundTripTestCase(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000.001, true, "'test'")))
                .add(new RoundTripTestCase(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000, 1000.001, true, "'test'")))
                .build();
    }

    protected static final class RoundTripTestCase
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<Object> fieldValues;
        private final int length;

        public RoundTripTestCase(String tableName, List<String> fieldNames, List<Object> fieldValues)
        {
            checkArgument(fieldNames.size() == fieldValues.size(), "sizes of fieldNames and fieldValues are not equal");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.fieldValues = ImmutableList.copyOf(fieldValues);
            this.length = fieldNames.size();
        }

        public String getTableName()
        {
            return tableName;
        }

        private int getIndex(String fieldName)
        {
            return fieldNames.indexOf(fieldName);
        }

        public String getFieldName(String fieldName)
        {
            int index = getIndex(fieldName);
            checkArgument(index >= 0 && index < length, "index out of bounds");
            return fieldNames.get(index);
        }

        public String getFieldNames()
        {
            return String.join(", ", fieldNames);
        }

        public Object getFieldValue(String fieldName)
        {
            int index = getIndex(fieldName);
            checkArgument(index >= 0 && index < length, "index out of bounds");
            return fieldValues.get(index);
        }

        public String getFieldValues()
        {
            return fieldValues.stream().map(Object::toString).collect(Collectors.joining(", "));
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
