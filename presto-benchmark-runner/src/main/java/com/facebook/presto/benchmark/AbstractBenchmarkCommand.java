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
package com.facebook.presto.benchmark;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.event.EventClientModule;
import com.facebook.presto.benchmark.framework.BenchmarkModule;
import com.facebook.presto.benchmark.source.BenchmarkCommand;
import com.facebook.presto.benchmark.source.BenchmarkSuiteModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;

import static com.google.common.base.Throwables.throwIfUnchecked;

public abstract class AbstractBenchmarkCommand
        implements BenchmarkCommand
{
    private static final Logger log = Logger.get(AbstractBenchmarkCommand.class);

    @Override
    public void run()
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new BenchmarkSuiteModule(getCustomBenchmarkSuiteSupplierTypes()))
                .add(new BenchmarkModule(
                        getSqlParserOptions(),
                        getParsingOptions(),
                        getExceptionClassifier()))
                .add(new EventClientModule(ImmutableSet.of()))
                .addAll(getAdditionalModules())
                .build());

        Injector injector = null;
        try {
            injector = app.initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            if (injector != null) {
                try {
                    injector.getInstance(LifeCycleManager.class).stop();
                }
                catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }
}
