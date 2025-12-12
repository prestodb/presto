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
package com.facebook.presto.sidecar.expressions;

import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class NativeExpressionsModule
        implements Module
{
    private final NodeManager nodeManager;
    private final RowExpressionSerde rowExpressionSerde;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution functionResolution;

    public NativeExpressionsModule(NodeManager nodeManager, RowExpressionSerde rowExpressionSerde, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution functionResolution)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public void configure(Binder binder)
    {
        // Core dependencies
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(RowExpressionSerde.class).toInstance(rowExpressionSerde);
        binder.bind(FunctionMetadataManager.class).toInstance(functionMetadataManager);
        binder.bind(StandardFunctionResolution.class).toInstance(functionResolution);

        // JSON dependencies and setup
        binder.install(new JsonModule());
        jsonBinder(binder).addDeserializerBinding(RowExpression.class).to(RowExpressionDeserializer.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(RowExpression.class).to(RowExpressionSerializer.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindListJsonCodec(RowExpression.class);
        jsonCodecBinder(binder).bindListJsonCodec(RowExpressionOptimizationResult.class);

        binder.bind(NativeSidecarExpressionInterpreter.class).in(Scopes.SINGLETON);

        // The main service provider
        binder.bind(NativeExpressionOptimizer.class).in(Scopes.SINGLETON);
    }
}
