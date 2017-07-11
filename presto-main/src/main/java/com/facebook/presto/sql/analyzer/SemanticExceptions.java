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

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

public final class SemanticExceptions
{
    private SemanticExceptions() {}

    public static SemanticException missingAttributeException(Expression node, QualifiedName name)
    {
        throw new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", name);
    }

    /**
     * Use {@link #missingAttributeException(Expression, QualifiedName)} instead.
     */
    @Deprecated
    public static SemanticException missingAttributeException(Expression node)
    {
        throw new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", node);
    }

    public static SemanticException ambiguousAttributeException(Expression node, QualifiedName name)
    {
        throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", name);
    }

    public static SemanticException notSupportedException(Node node, String notSupportedFeatureDescription)
    {
        throw new SemanticException(NOT_SUPPORTED, node, notSupportedFeatureDescription + " is not supported");
    }
}
