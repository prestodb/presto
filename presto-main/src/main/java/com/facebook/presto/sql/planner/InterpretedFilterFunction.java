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
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

import static java.lang.Boolean.TRUE;

public class InterpretedFilterFunction
        implements FilterFunction
{
    private final Expression predicate;
    private final TupleInputResolver resolver = new TupleInputResolver();
    private final ExpressionInterpreter evaluator;

    private final Map<Input, Type> inputTypes;

    public InterpretedFilterFunction(Expression predicate, Map<Symbol, Input> symbolToInputMappings, Metadata metadata, Session session, Map<Input, Type> inputTypes)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        this.predicate = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), predicate);
        evaluator = ExpressionInterpreter.expressionInterpreter(resolver, metadata, session);
        this.inputTypes = inputTypes;
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        resolver.setInputs(cursors);
        return filter();
    }

    @Override
    public boolean filter(RecordCursor cursor)
    {
        resolver.setCursor(cursor, inputTypes);
        return filter();
    }

    private boolean filter()
    {
        Object result = evaluator.process(predicate, null);
        return result == TRUE;
    }
}
