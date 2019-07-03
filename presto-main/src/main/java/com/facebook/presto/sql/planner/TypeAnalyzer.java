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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;

/**
 * This class is to facilitate obtaining the type of an expression and its subexpressions
 * during planning (i.e., when interacting with IR expression). It will eventually get
 * removed when we split the AST from the IR and we encode the type directly into IR expressions.
 */
public class TypeAnalyzer
{
    private final SqlParser parser;
    private final Metadata metadata;

    @Inject
    public TypeAnalyzer(SqlParser parser, Metadata metadata)
    {
        this.parser = parser;
        this.metadata = metadata;
    }

    private Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, Iterable<Expression> expressions)
    {
        return ExpressionAnalyzer.analyzeExpressions(session, metadata, parser, inputTypes, expressions, ImmutableList.of(), WarningCollector.NOOP, false).getExpressionTypes();
    }

    public Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, Expression expression)
    {
        return getTypes(session, inputTypes, ImmutableList.of(expression));
    }
}
