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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.log.Level;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.delta.TestDeltaIntegration;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.BeforeClass;

public class TestPrestoNativeDeltaIntegration
        extends TestDeltaIntegration
{
    @Override
    protected String goldenTablePath(String tableName)
    {
        return extractedGoldenTablePath(tableName);
    }

    @BeforeClass
    public static void silenceDeltaLogging()
    {
        // Hide huge warning logs caused by not having checkpoints.
        Logging logging = Logging.initialize();
        logging.setLevel("io.delta.kernel", Level.ERROR);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeDeltaQueryRunnerBuilder()
                .build();
        // Create the test Delta tables in HMS
        for (String deltaTestTable : DELTA_TEST_TABLE_LIST) {
            registerDeltaTableInHMS(queryRunner, deltaTestTable, deltaTestTable);
        }
        return queryRunner;
    }
}
