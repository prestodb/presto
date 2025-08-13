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
package com.facebook.presto.spark;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.google.common.collect.Streams;
import jakarta.inject.Inject;

import javax.inject.Provider;

import java.util.Map;

import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkSessionPropertyManagerProvider
        implements Provider<SessionPropertyManager>
{
    private final SystemSessionProperties systemSessionProperties;
    private final PrestoSparkSessionProperties prestoSparkSessionProperties;
    private final Map<String, WorkerSessionPropertyProvider> workerSessionPropertyProviders;
    private final JavaFeaturesConfig javaFeaturesConfig;
    private final NodeSpillConfig nodeSpillConfig;

    @Inject
    public PrestoSparkSessionPropertyManagerProvider(
            SystemSessionProperties systemSessionProperties,
            PrestoSparkSessionProperties prestoSparkSessionProperties,
            Map<String, WorkerSessionPropertyProvider> workerSessionPropertyProviders,
            JavaFeaturesConfig javaFeaturesConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this.systemSessionProperties = requireNonNull(systemSessionProperties,
            "systemSessionProperties is null");
        this.prestoSparkSessionProperties = requireNonNull(prestoSparkSessionProperties,
            "prestoSparkSessionProperties is null");
        this.workerSessionPropertyProviders = requireNonNull(workerSessionPropertyProviders,
            "workerSessionPropertyProviders is null");
        this.javaFeaturesConfig = requireNonNull(javaFeaturesConfig, "javaFeaturesConfig is null");
        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
    }

    @Override
    public SessionPropertyManager get()
    {
        return createTestingSessionPropertyManager(
            Streams.concat(
                systemSessionProperties.getSessionProperties().stream(),
                prestoSparkSessionProperties.getSessionProperties().stream(),
                workerSessionPropertyProviders.values().stream()
                    .flatMap(provider -> provider.getSessionProperties().stream())
            ).collect(toImmutableList()),
            javaFeaturesConfig,
            nodeSpillConfig);
    }
}
