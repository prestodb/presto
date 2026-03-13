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
package com.facebook.presto.sidecar.nativechecker;

import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class NativePlanCheckerModule
        implements Module
{
    private final NodeManager nodeManager;
    private final SimplePlanFragmentSerde simplePlanFragmentSerde;

    public NativePlanCheckerModule(NodeManager nodeManager, SimplePlanFragmentSerde simplePlanFragmentSerde)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.simplePlanFragmentSerde = requireNonNull(simplePlanFragmentSerde, "simplePlanFragmentSerde is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(NativePlanCheckerConfig.class, NativePlanCheckerConfig.CONFIG_PREFIX);
        binder.install(new JsonModule());
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(SimplePlanFragmentSerde.class).toInstance(simplePlanFragmentSerde);
        jsonBinder(binder).addSerializerBinding(SimplePlanFragment.class).to(SimplePlanFragmentSerializer.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(SimplePlanFragment.class);
        binder.bind(NativePlanChecker.class).in(Scopes.SINGLETON);
        binder.bind(PlanCheckerProvider.class).to(NativePlanCheckerProvider.class).in(Scopes.SINGLETON);
    }
}
