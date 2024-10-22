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
package com.facebook.presto.sidecar.sessionpropertyproviders;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NativeSystemSessionPropertyProviderModule
        implements Module
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    public NativeSystemSessionPropertyProviderModule(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<JsonCodec<List<SessionPropertyMetadata>>>() {})
                .toInstance(new JsonCodecFactory().listJsonCodec(SessionPropertyMetadata.class));
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NativeSystemSessionPropertyProviderConfig.class).in(Scopes.SINGLETON);
        binder.bind(NativeSystemSessionPropertyProvider.class).in(Scopes.SINGLETON);
        binder.bind(WorkerSessionPropertyProvider.class).to(NativeSystemSessionPropertyProvider.class).in(Scopes.SINGLETON);
    }
}
