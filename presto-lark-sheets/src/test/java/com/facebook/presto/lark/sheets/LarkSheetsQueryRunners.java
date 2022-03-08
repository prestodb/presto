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

package com.facebook.presto.lark.sheets;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.lark.sheets.LarkSheetsConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.lark.sheets.TestLarkSheetsPlugin.getTestingConnectorConfig;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

class LarkSheetsQueryRunners
{
    private LarkSheetsQueryRunners() {}

    static DistributedQueryRunner createSheetsQueryRunner(
            String catalog,
            Map<String, String> extraProperties,
            Map<String, String> catalogProperties)
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(catalog).build();
        LarkSheetsPlugin larkSheetsPlugin = new LarkSheetsPlugin();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(larkSheetsPlugin);
        queryRunner.createCatalog(catalog, CONNECTOR_NAME, catalogProperties);
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createSheetsQueryRunner(
                "larksheets",
                ImmutableMap.of("http-server.http.port", "8080"),
                getTestingConnectorConfig());
        Thread.sleep(10);
        Logger log = Logger.get(TestLarkSheetsIntegration.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
