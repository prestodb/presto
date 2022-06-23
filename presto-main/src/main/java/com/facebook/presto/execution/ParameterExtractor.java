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
package com.facebook.presto.execution;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;

public class ParameterExtractor
{
    private ParameterExtractor() {}

    public static int getParameterCount(Statement statement)
    {
        ParameterExtractingVisitor parameterExtractingVisitor = new ParameterExtractingVisitor();
        parameterExtractingVisitor.process(statement, null);
        return parameterExtractingVisitor.getParameters().size();
    }

    public static List<Parameter> getParameters(Statement statement)
    {
        ParameterExtractingVisitor parameterExtractingVisitor = new ParameterExtractingVisitor();
        parameterExtractingVisitor.process(statement, null);
        return parameterExtractingVisitor.getParameters();
    }

    private static class ParameterExtractingVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final List<Parameter> parameters = new ArrayList<>();

        public List<Parameter> getParameters()
        {
            return parameters;
        }

        @Override
        public Void visitParameter(Parameter node, Void context)
        {
            parameters.add(node);
            return null;
        }

        @Override
        protected Void visitLambdaExpression(LambdaExpression node, Void context)
        {
            process(node.getBody(), context);
            for (LambdaArgumentDeclaration argument : node.getArguments()) {
                process(argument, context);
            }
            return null;
        }
    }
}
