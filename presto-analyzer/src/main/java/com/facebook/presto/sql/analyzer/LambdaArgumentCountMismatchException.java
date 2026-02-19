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

import com.facebook.presto.sql.tree.Node;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static java.lang.String.format;

public class LambdaArgumentCountMismatchException
        extends SemanticException
{
    public LambdaArgumentCountMismatchException(Node node, int expectedCount, int actualCount)
    {
        super(INVALID_PARAMETER_USAGE, node,
                format("Expected a lambda that takes %s argument(s) but got %s", expectedCount, actualCount));
    }
}