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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;

public class HiveExternalWorkerQueryRunner
{
    private HiveExternalWorkerQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work.
        Logging.initialize();

        // Create tables before launching distributed runner.
        QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
        NativeQueryRunnerUtils.createAllTables(javaQueryRunner);
        javaQueryRunner.close();

        // Launch distributed runner.
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunner();
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
