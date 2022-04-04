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

import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;

import java.nio.file.Paths;
import java.util.Optional;

import static org.testng.Assert.assertNotNull;

public class AbstractTestHiveQueries
        extends AbstractTestQueryFramework
{
    private final boolean useThrift;

    protected AbstractTestHiveQueries(boolean useThrift)
    {
        this.useThrift = useThrift;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String baseDataDir = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath);
        assertNotNull(baseDataDir);

        return HiveExternalWorkerQueryRunner.createNativeQueryRunner(baseDataDir, prestoServerPath, Optional.ofNullable(workerCount).map(Integer::parseInt), cacheMaxSize, useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String baseDataDir = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.of(Paths.get(baseDataDir)));
    }
}
