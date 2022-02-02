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
package com.facebook.presto.functionNamespace.execution.grpc;

import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.util.Providers;

import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;

public class GrpcSqlFunctionExecutorsModule
        implements Module
{
    private final Map<RoutineCharacteristics.Language, GrpcSqlFunctionExecutionConfig> supportedLanguages;

    public GrpcSqlFunctionExecutorsModule(Map<RoutineCharacteristics.Language, GrpcSqlFunctionExecutionConfig> supportedLanguages)
    {
        this.supportedLanguages = supportedLanguages;
    }

    @Override
    public void configure(Binder binder)
    {
        if (supportedLanguages.isEmpty()) {
            binder.bind(GrpcSqlFunctionExecutor.class).toProvider(Providers.of(null));
            return;
        }
        binder.bind(GrpcSqlFunctionExecutor.class).in(SINGLETON);
    }
}
