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

import com.facebook.presto.sidecar.NativeSidecarFailureInfo;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class RowExpressionOptimizationResult
{
    private final RowExpression optimizedExpression;
    private final NativeSidecarFailureInfo expressionFailureInfo;

    @JsonCreator
    public RowExpressionOptimizationResult(
            @JsonProperty("optimizedExpression") RowExpression optimizedExpression,
            @JsonProperty("expressionFailureInfo") NativeSidecarFailureInfo expressionFailureInfo)
    {
        this.optimizedExpression = optimizedExpression;
        this.expressionFailureInfo = expressionFailureInfo;
    }

    public RowExpression getOptimizedExpression()
    {
        return optimizedExpression;
    }

    public NativeSidecarFailureInfo getExpressionFailureInfo()
    {
        return expressionFailureInfo;
    }
}
