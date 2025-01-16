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

package com.facebook.presto.sql.planner.sanity;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanCheckerProviderContext;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.sql.planner.plan.JsonCodecSimplePlanFragmentSerde;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.sql.planner.sanity.TestPlanCheckerProviderManager.TestingPlanCheckerProvider.TESTING_PLAN_CHECKER_PROVIDER;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPlanCheckerProviderManager
{
    @Test
    public void testLoadPlanCheckerProviders()
            throws IOException
    {
        PlanCheckerProviderManagerConfig planCheckerProviderManagerConfig = new PlanCheckerProviderManagerConfig()
                .setPlanCheckerConfigurationDir(new File("src/test/resources/plan-checkers"));
        PlanCheckerProviderManager planCheckerProviderManager = new PlanCheckerProviderManager(new JsonCodecSimplePlanFragmentSerde(JsonCodec.jsonCodec(SimplePlanFragment.class)), planCheckerProviderManagerConfig);
        planCheckerProviderManager.addPlanCheckerProviderFactory(new TestingPlanCheckerProviderFactory());
        planCheckerProviderManager.loadPlanCheckerProviders(new TestingNodeManager());
        assertEquals(planCheckerProviderManager.getPlanCheckerProviders(), ImmutableList.of(TESTING_PLAN_CHECKER_PROVIDER));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No planCheckerProviderFactory found for 'test'. Available factories were \\[]")
    public void testLoadUnregisteredPlanCheckerProvider()
            throws IOException
    {
        PlanCheckerProviderManagerConfig planCheckerProviderManagerConfig = new PlanCheckerProviderManagerConfig()
                .setPlanCheckerConfigurationDir(new File("src/test/resources/plan-checkers"));
        PlanCheckerProviderManager planCheckerProviderManager = new PlanCheckerProviderManager(new JsonCodecSimplePlanFragmentSerde(JsonCodec.jsonCodec(SimplePlanFragment.class)), planCheckerProviderManagerConfig);
        planCheckerProviderManager.loadPlanCheckerProviders(new TestingNodeManager());
    }

    public static class TestingPlanCheckerProviderFactory
            implements PlanCheckerProviderFactory
    {
        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public PlanCheckerProvider create(Map<String, String> properties, PlanCheckerProviderContext planCheckerProviderContext)
        {
            return TESTING_PLAN_CHECKER_PROVIDER;
        }
    }

    public static class TestingPlanCheckerProvider
            implements PlanCheckerProvider
    {
        public static final TestingPlanCheckerProvider TESTING_PLAN_CHECKER_PROVIDER = new TestingPlanCheckerProvider();
    }
}
