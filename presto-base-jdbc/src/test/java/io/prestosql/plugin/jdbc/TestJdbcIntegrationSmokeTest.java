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
package io.prestosql.plugin.jdbc;

import io.prestosql.tests.AbstractTestIntegrationSmokeTest;

import static io.airlift.tpch.TpchTable.ORDERS;
import static io.prestosql.plugin.jdbc.JdbcQueryRunner.createJdbcQueryRunner;

public class TestJdbcIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestJdbcIntegrationSmokeTest()
    {
        super(() -> createJdbcQueryRunner(ORDERS));
    }
}
