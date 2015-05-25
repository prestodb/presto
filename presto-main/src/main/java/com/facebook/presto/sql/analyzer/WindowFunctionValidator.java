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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.FunctionCall;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_REQUIRES_OVER;
import static com.google.common.base.Preconditions.checkNotNull;

public class WindowFunctionValidator
        extends DefaultExpressionTraversalVisitor<Void, Analysis>
{
    @Override
    protected Void visitFunctionCall(FunctionCall functionCall, Analysis analysis)
    {
        checkNotNull(analysis, "analysis is null");

        FunctionInfo functionInfo = analysis.getFunctionInfo(functionCall);
        if (functionInfo != null && functionInfo.isWindow() && !functionInfo.isAggregate() && !functionCall.getWindow().isPresent()) {
            throw new SemanticException(WINDOW_REQUIRES_OVER, functionCall, "Window function %s requires an OVER clause", functionInfo.getName());
        }
        return super.visitFunctionCall(functionCall, analysis);
    }
}
