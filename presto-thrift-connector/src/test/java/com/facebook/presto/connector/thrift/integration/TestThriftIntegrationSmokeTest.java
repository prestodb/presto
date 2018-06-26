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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.connector.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.QueryAssertions.assertContains;

public class TestThriftIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestThriftIntegrationSmokeTest()
    {
        super(() -> createThriftQueryRunner(2, 2, false, ImmutableMap.of()));
    }

    @Override
    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row("tiny")
                .row("sf1");
        assertContains(actualSchemas, resultBuilder.build());
    }
}
