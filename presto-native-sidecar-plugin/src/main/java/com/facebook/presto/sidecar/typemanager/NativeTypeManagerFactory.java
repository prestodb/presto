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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.type.TypeManagerContext;
import com.facebook.presto.spi.type.TypeManagerFactory;
import com.facebook.presto.spi.type.TypeManagerProvider;
import com.google.inject.Injector;

public class NativeTypeManagerFactory
        implements TypeManagerFactory
{
    public static final String NAME = "native";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public TypeManagerProvider create(TypeManagerContext context)
    {
        Bootstrap app = new Bootstrap(
                new NativeTypeManagerModule(context.getFunctionMetadataManager()));

        Injector injector = app
                .doNotInitializeLogging()
                .noStrictConfig()
                .initialize();
        return injector.getInstance(NativeTypeManager.class);
    }
}
