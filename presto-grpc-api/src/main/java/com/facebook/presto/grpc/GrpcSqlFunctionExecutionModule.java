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
package com.facebook.presto.grpc;

import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutionModule;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import java.util.Map;

import static com.facebook.presto.spi.function.FunctionImplementationType.GRPC;

public class GrpcSqlFunctionExecutionModule
        extends SqlFunctionExecutionModule
{
    @Override
    protected void setup(Binder binder)
    {
        ImmutableMap.Builder<Language, GrpcSqlFunctionExecutionConfig> grpcUdfConfigBuilder = ImmutableMap.builder();
        for (Map.Entry<String, FunctionImplementationType> entry : supportedLanguages.entrySet()) {
            String languageName = entry.getKey();
            Language language = new Language(languageName);
            FunctionImplementationType implementationType = entry.getValue();
            if (implementationType.equals(GRPC)) {
                grpcUdfConfigBuilder.put(language, buildConfigObject(GrpcSqlFunctionExecutionConfig.class, languageName));
            }
        }

        Map<Language, GrpcSqlFunctionExecutionConfig> grpcUdfConfigs = grpcUdfConfigBuilder.build();
        if (grpcUdfConfigs.isEmpty()) {
            binder.bind(SqlFunctionExecutor.class).to(NoopSqlFunctionExecutor.class).in(Scopes.SINGLETON);
            return;
        }
        binder.bind(SqlFunctionExecutor.class).to(GrpcSqlFunctionExecutor.class).in(Scopes.SINGLETON);
        binder.bind(new TypeLiteral<Map<Language, GrpcSqlFunctionExecutionConfig>>() {}).toInstance(grpcUdfConfigs);
    }
}
