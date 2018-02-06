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
package com.facebook.presto.sql.gen;

import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.MethodDefinition;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PreGeneratedExpressions
{
    private final Map<CallExpression, MethodDefinition> tryMethodMap;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;

    public PreGeneratedExpressions(Map<CallExpression, MethodDefinition> tryMethodMap, Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this.tryMethodMap = ImmutableMap.copyOf(requireNonNull(tryMethodMap, "tryMethodMap is null"));
        this.compiledLambdaMap = ImmutableMap.copyOf(requireNonNull(compiledLambdaMap, "compiledLambdaMap is null"));
    }

    public Map<CallExpression, MethodDefinition> getTryMethodMap()
    {
        return tryMethodMap;
    }

    public Map<LambdaDefinitionExpression, CompiledLambda> getCompiledLambdaMap()
    {
        return compiledLambdaMap;
    }
}
