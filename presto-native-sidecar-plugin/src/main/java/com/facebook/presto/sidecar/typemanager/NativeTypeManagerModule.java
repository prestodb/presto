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
package com.facebook.presto.sidecar.typemanager;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.type.TypeManagerProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NativeTypeManagerModule
        implements Module
{
    private final FunctionMetadataManager functionMetadataManager;

    public NativeTypeManagerModule(FunctionMetadataManager functionMetadataManager)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<JsonCodec<List<SessionPropertyMetadata>>>() {})
                .toInstance(new JsonCodecFactory().listJsonCodec(SessionPropertyMetadata.class));
        binder.bind(FunctionMetadataManager.class).toInstance(functionMetadataManager);
        binder.bind(NativeTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TypeManagerProvider.class).to(NativeTypeManager.class).in(Scopes.SINGLETON);
    }
}
