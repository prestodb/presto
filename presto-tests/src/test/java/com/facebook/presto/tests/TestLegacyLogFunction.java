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
package com.facebook.presto.tests;

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestLegacyLogFunction
{
    private static final String QUERY = "SELECT LOG(25, 5)";

    @Test
    public void testLegacyLogFunctionEnabled()
    {
        try (QueryRunner queryRunner = createQueryRunner(true)) {
            MaterializedResult result = queryRunner.execute(QUERY);
            assertEquals(result.getOnlyValue(), 2.0);
        }
    }

    @Test(expectedExceptions = {SemanticException.class},
            expectedExceptionsMessageRegExp = ".*Function log not registered")
    public void testLegacyLogFunctionDisabled()
    {
        try (QueryRunner runner = createQueryRunner(false)) {
            runner.execute(QUERY);
        }
    }

    private static QueryRunner createQueryRunner(boolean legacyLogFunction)
    {
        return new LocalQueryRunner(testSessionBuilder().build(),
                new FeaturesConfig().setLegacyLogFunction(legacyLogFunction));
    }
}
