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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestLocalBinarySpilledQueries
        extends AbstractTestQueries
{
    public TestLocalBinarySpilledQueries()
    {
        super(TestLocalBinarySpilledQueries::createLocalQueryRunner);
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.OPERATOR_MEMORY_LIMIT_BEFORE_SPILL, "1B") //spill constantly
                .build();

        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString());
        featuresConfig.setOptimizeMixedDistinctAggregations(true);
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession, featuresConfig);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }
}
