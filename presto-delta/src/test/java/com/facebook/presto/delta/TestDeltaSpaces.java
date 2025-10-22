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

import org.testng.annotations.Test;

import static java.lang.String.format;

/**
 * Tests for reading Delta tables with spaces in location or partition.
 */
@Test
public class TestDeltaSpaces
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test(dataProvider = "deltaReaderVersions")
    public void readDataWithSpaces(String version)
    {
        // Tests for reading Delta tables with spaces in location or partition.
        String testQuery =
                format("SELECT * FROM \"%s\".\"%s\" WHERE country = 'SOUTH AFRICA'", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        "test-spaces"));

        // read snapshot version 3
        String testQueryV3 = format(testQuery, "v3");
        String expResultsQueryV3 = "SELECT * FROM VALUES('Chima', 'manda', 'SOUTH AFRICA'), ('Adi', 'chie', 'SOUTH AFRICA')";
        assertQuery(testQueryV3, expResultsQueryV3);
    }
}
