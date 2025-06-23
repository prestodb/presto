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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class PlanCheckerRouterPluginSchedulerFactory
        implements SchedulerFactory
{
    public static final String PLAN_CHECKER_ROUTER_PLUGIN = "plan-checker";

    @Override
    public String getName()
    {
        return PLAN_CHECKER_ROUTER_PLUGIN;
    }

    @Override
    public Scheduler create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(new PlanCheckerRouterPluginModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            PlanCheckerRouterPluginScheduler planCheckerRouterPluginScheduler =
                    injector.getInstance(PlanCheckerRouterPluginScheduler.class);
            CounterStat javaRedirectCounter = injector.getInstance(Key.get(CounterStat.class, JavaClusterRedirectRequestsCounter.class));
            CounterStat nativeRedirectCounter = injector.getInstance(Key.get(CounterStat.class, NativeClustersRedirectRequestsCounter.class));

            JmxRegistrationUtil.register("com.facebook.presto.router.scheduler", "Java", new ExportedPlanCheckerRouterPluginCounter(javaRedirectCounter));
            JmxRegistrationUtil.register("com.facebook.presto.router.scheduler", "Native", new ExportedPlanCheckerRouterPluginCounter(nativeRedirectCounter));

            return planCheckerRouterPluginScheduler;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
