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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

import static java.lang.Boolean.TRUE;

public class InterpretedFilterFunction
        implements FilterFunction
{
    private final ExpressionInterpreter evaluator;

    public InterpretedFilterFunction(Expression predicate, Map<Symbol, Input> symbolToInputMappings, Metadata metadata, Session session)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), predicate);
        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session);
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        return evaluator.evaluate(cursors) == TRUE;
    }

    @Override
    public boolean filter(RecordCursor cursor)
    {
        return evaluator.evaluate(cursor) == TRUE;
    }
}
