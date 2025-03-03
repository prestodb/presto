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

import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.ExpressionOptimizer;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class NativeExpressionOptimizerProvider
{
    private final NativeSidecarExpressionInterpreter expressionInterpreterService;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution resolution;

    @Inject
    public NativeExpressionOptimizerProvider(NativeSidecarExpressionInterpreter expressionInterpreterService, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution resolution)
    {
        this.expressionInterpreterService = requireNonNull(expressionInterpreterService, "expressionInterpreterService is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.resolution = requireNonNull(resolution, "resolution is null");
    }

    public ExpressionOptimizer createOptimizer()
    {
        return new NativeExpressionOptimizer(expressionInterpreterService, functionMetadataManager, resolution);
    }
}
