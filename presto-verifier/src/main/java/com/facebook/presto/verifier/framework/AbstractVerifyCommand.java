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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.event.EventClientModule;
import com.facebook.presto.verifier.source.SourceQueryModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.airline.Arguments;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import static com.google.common.base.Throwables.throwIfUnchecked;

public abstract class AbstractVerifyCommand
        implements VerifyCommand
{
    private static final Logger log = Logger.get(AbstractVerifyCommand.class);

    @Arguments(description = "Config filename")
    public String configFilename;

    @Override
    public void run()
    {
        if (configFilename != null) {
            System.setProperty("config", configFilename);
        }

        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new VerifierModule(
                        getSqlParserOptions(),
                        getCustomQueryFilterClasses(),
                        getSqlExceptionClassifier(),
                        getFailureResolvers(),
                        getTablePropertyOverrides()))
                .add(new SourceQueryModule(getCustomSourceQuerySupplierTypes()))
                .add(new EventClientModule(getCustomEventClientTypes()))
                .addAll(getAdditionalModules())
                .build());
        Injector injector = null;
        try {
            injector = app.strictConfig().initialize();
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
