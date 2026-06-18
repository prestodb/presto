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
package com.facebook.presto.spark.classloader_interface;

import com.facebook.airlift.units.Duration;
import com.google.common.base.Ticker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class PrestoSparkBootstrapTimer
{
    private final Ticker ticker;
    private final boolean isExecutor;

    private final AtomicReference<Long> beginRunnerServiceCreation = new AtomicReference<>();
    private final AtomicReference<Duration> endRunnerServiceCreation = new AtomicReference<>();
    private final AtomicReference<Long> beginPrestoSparkServiceCreation = new AtomicReference<>();
    private final AtomicReference<Duration> endPrestoSparkServiceCreation = new AtomicReference<>();
    private final AtomicReference<Long> beginInjectorCreation = new AtomicReference<>();
    private final AtomicReference<Duration> endInjectorCreation = new AtomicReference<>();
    private final AtomicReference<Long> beginInjectorInitialization = new AtomicReference<>();
    private final AtomicReference<Duration> endInjectorInitialization = new AtomicReference<>();
    private final AtomicReference<Long> beginSharedModulesLoading = new AtomicReference<>();
    private final AtomicReference<Duration> endSharedModulesLoading = new AtomicReference<>();
    private final AtomicReference<Long> beginNonTestingModulesLoading = new AtomicReference<>();
    private final AtomicReference<Duration> endNonTestingModulesLoading = new AtomicReference<>();
    private final AtomicReference<Long> beginDriverModulesLoading = new AtomicReference<>();
    private final AtomicReference<Duration> endDriverModulesLoading = new AtomicReference<>();

    public PrestoSparkBootstrapTimer(Ticker ticker, boolean isExecutor)
    {
        this.ticker = requireNonNull(ticker, "PrestoSparkBootstrapTimer ticker is null");
        this.isExecutor = isExecutor;
    }

    public Map<String, Long> exportBootstrapDurations()
    {
        Map<String, Long> output = new HashMap<>();
        output.put("PrestoSparkServiceCreationDurationMS",
                (endPrestoSparkServiceCreation.get() != null) ? endPrestoSparkServiceCreation.get().toMillis() : 0);
        output.put("RunnerServiceCreationDurationMS",
                (endRunnerServiceCreation.get() != null) ? endRunnerServiceCreation.get().toMillis() : 0);
        output.put("InjectorCreationDurationMS",
                (endInjectorCreation.get() != null) ? endInjectorCreation.get().toMillis() : 0);
        output.put("InjectorInitializationDurationMS",
                (endInjectorInitialization.get() != null) ? endInjectorCreation.get().toMillis() : 0);
        output.put("SharedModulesLoadingDurationMS",
                (endSharedModulesLoading.get() != null) ? endSharedModulesLoading.get().toMillis() : 0);
        output.put("NonTestingModulesLoadingDurationMS",
                (endNonTestingModulesLoading.get() != null) ? endNonTestingModulesLoading.get().toMillis() : 0);
        output.put("DriverModulesLoadingDurationMS",
                (endDriverModulesLoading.get() != null) ? endDriverModulesLoading.get().toMillis() : 0);
        return output;
    }

    public boolean isExecutorBootstrap()
    {
        return isExecutor;
    }

    private static Duration nanosSince(AtomicReference<Long> start, long end)
    {
        Long startNanos = start.get();
        if (startNanos == null) {
            throw new IllegalStateException("Start time not set");
        }
        return nanosSince(startNanos, end);
    }

    private static Duration nanosSince(long start, long now)
    {
        return succinctNanos(max(0, now - start));
    }

    // Runner
    public void beginRunnerServiceCreation()
    {
        beginRunnerServiceCreation.compareAndSet(null, ticker.read());
    }

    public void endRunnerServiceCreation()
    {
        endRunnerServiceCreation.compareAndSet(null, nanosSince(beginRunnerServiceCreation, ticker.read()));
    }

    public void beginPrestoSparkServiceCreation()
    {
        beginPrestoSparkServiceCreation.compareAndSet(null, ticker.read());
    }

    public void endPrestoSparkServiceCreation()
    {
        endPrestoSparkServiceCreation.compareAndSet(null, nanosSince(beginPrestoSparkServiceCreation, ticker.read()));
    }

    public void beginInjectorCreation()
    {
        beginInjectorCreation.compareAndSet(null, ticker.read());
    }

    public void endInjectorCreation()
    {
        endInjectorCreation.compareAndSet(null, nanosSince(beginInjectorCreation, ticker.read()));
    }

    public void beginInjectorInitialization()
    {
        beginInjectorInitialization.compareAndSet(null, ticker.read());
    }

    public void endInjectorInitialization()
    {
        endInjectorInitialization.compareAndSet(null, nanosSince(beginInjectorInitialization, ticker.read()));
    }

    public void beginSharedModulesLoading()
    {
        beginSharedModulesLoading.compareAndSet(null, ticker.read());
    }

    public void endSharedModulesLoading()
    {
        endSharedModulesLoading.compareAndSet(null, nanosSince(beginSharedModulesLoading, ticker.read()));
    }

    public void beginNonTestingModulesLoading()
    {
        beginNonTestingModulesLoading.compareAndSet(null, ticker.read());
    }

    public void endNonTestingModulesLoading()
    {
        endNonTestingModulesLoading.compareAndSet(null, nanosSince(beginNonTestingModulesLoading, ticker.read()));
    }

    public void beginDriverModulesLoading()
    {
        beginDriverModulesLoading.compareAndSet(null, ticker.read());
    }

    public void endDriverModulesLoading()
    {
        endDriverModulesLoading.compareAndSet(null, nanosSince(beginDriverModulesLoading, ticker.read()));
    }
}
