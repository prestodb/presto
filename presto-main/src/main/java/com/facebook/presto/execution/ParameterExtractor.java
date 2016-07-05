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
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Statement;

public class ParameterExtractor
{
    private ParameterExtractor(){}

    public static int getParameterCount(Statement statement)
    {
        ParameterExtractingVisitor parameterExtractingVisitor = new ParameterExtractingVisitor();
        parameterExtractingVisitor.process(statement, null);
        return parameterExtractingVisitor.getParameterCount();
    }

    private static class ParameterExtractingVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private int parameterCount = 0;

        private int getParameterCount()
        {
            return parameterCount;
        }

        @Override
        public Void visitParameter(Parameter node, Void context)
        {
            parameterCount++;
            return null;
        }
    }
}
