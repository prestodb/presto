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
package com.facebook.presto.runtimestats;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeStatsInstrument;
import com.facebook.presto.spi.RuntimeStatsInstrumentFactory;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuntimeStatsManager
{
    private static final Logger log = Logger.get(RuntimeStatsManager.class);
    private final List<RuntimeStatsInstrumentFactory> runtimeStatsInstrumentFactories = new ArrayList<>();
    private final List<RuntimeStatsInstrument> runtimeStatsInstruments = new ArrayList<>();

    @Inject
    public RuntimeStatsManager() {}

    public void addRuntimeStatsInstrumentFactory(RuntimeStatsInstrumentFactory runtimeStatsInstrumentFactories)
    {
        this.runtimeStatsInstrumentFactories.add(runtimeStatsInstrumentFactories);
    }

    public static RuntimeStats newRuntimeStats()
    {
        return new RuntimeStats();
    }

    public RuntimeStats newRuntimeStatsWithInstruments()
    {
        return new RuntimeStats(runtimeStatsInstruments);
    }

    public void loadRuntimeStatsInstruments()
    {
        Map<String, String> config = new HashMap<>();
        for (RuntimeStatsInstrumentFactory runtimeStatsInstrumentFactory : runtimeStatsInstrumentFactories) {
            log.info("-- Loading runtime stats instrument factory %s --", runtimeStatsInstrumentFactory.getName());
            this.runtimeStatsInstruments.add(runtimeStatsInstrumentFactory.createRuntimeStatsInstrument(config));
            log.info("-- Loaded runtime stats instrument factory %s --", runtimeStatsInstrumentFactory.getName());
        }
    }
}
