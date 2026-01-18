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
package com.facebook.presto.delta;

import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestUppercasePartitionColumns
        extends AbstractDeltaDistributedQueryTestBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> extraProperties = ImmutableMap.of(
                "experimental.pushdown-subfields-enabled", "true",
                "experimental.pushdown-dereference-enabled", "true");

        return DeltaQueryRunner.builder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Europe/Madrid"))
                .setExtraProperties(extraProperties)
                .caseSensitivePartitions()
                .build()
                .getQueryRunner();
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readDeltaTableWithUpperCaseRegularColumnTest(String version)
    {
        String controlTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-lowercase"));
        String testTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-uppercase"));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readDeltaTableWithUpperCasePartitionValuesTest(String version)
    {
        String controlTableQuery = format("select * from \"%s\".\"%s\" order by id asc",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-partitions-lowercase"));
        String testTableQuery = format("select * from \"%s\".\"%s\" order by id asc", PATH_SCHEMA,
                goldenTablePathWithPrefix(version, "test-partitions-uppercase"));
        MaterializedResult expectedResult = getQueryRunner().execute(controlTableQuery);
        MaterializedResult testResult = getQueryRunner().execute(testTableQuery);
        assertEquals(testResult.getMaterializedRows(), expectedResult.getMaterializedRows());
    }
}
