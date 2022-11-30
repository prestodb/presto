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
package com.facebook.presto.analyzer.crux;

import com.facebook.presto.analyzer.crux.tree.EmptyQuery;
import com.facebook.presto.analyzer.crux.tree.Expression;
import com.facebook.presto.analyzer.crux.tree.Literal;
import com.facebook.presto.analyzer.crux.tree.LiteralExpression;
import com.facebook.presto.analyzer.crux.tree.PrimitiveType;
import com.facebook.presto.analyzer.crux.tree.SelectItem;
import com.facebook.presto.analyzer.crux.tree.SelectQuery;
import com.facebook.presto.analyzer.crux.tree.SemanticTree;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.analyzer.crux.tree.ExpressionCardinality.SCALAR;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCrux
{
    @Test
    public void testEmptyQuery()
    {
        SemanticTree tree = Crux.sqlToTree("");

        assertTrue(tree.isStatement());
        assertTrue(tree.asStatement().isQuery());
        assertTrue(tree.asStatement().asQuery().isEmptyQuery());
    }

    @Test
    public void testSimpleQuery()
    {
        testSelectLiteral("1");
        testSelectLiteral("5");
        testSelectLiteral("10");
        testSelectLiteral("25");
    }

    private void testSelectLiteral(String literalValue)
    {
        SemanticTree tree = getSemanticTreeForSelectLiteral(literalValue);

        assertTrue(tree.isStatement());
        assertTrue(tree.asStatement().isQuery());
        assertTrue(tree.asStatement().asQuery().isSelectQuery());

        List<SelectItem> selectItems = tree.asStatement().asQuery().asSelectQuery().getSelectItems();
        assertEquals(selectItems.size(), 1);
        Expression expression = selectItems.get(0).getValue();
        assertTrue(expression instanceof LiteralExpression);
        Literal literal = expression.asLiteralExpression().getValue();
        assertEquals(literal.getValue(), literalValue);

        Visitor visitor = new Visitor(literalValue);
        visitor.visitSemanticTree(tree);
    }

    private SemanticTree getSemanticTreeForSelectLiteral(String literalValue)
    {
        PrimitiveType primitiveType = new PrimitiveType();
        Literal value = new Literal(literalValue);
        LiteralExpression literalExpression = new LiteralExpression(null, primitiveType, SCALAR, value, "");
        List<String> names = singletonList("");
        SelectItem selectItem = new SelectItem(null, primitiveType, SCALAR, names, literalExpression, null);
        List<SelectItem> selectItems = singletonList(selectItem);

        EmptyQuery emptyQuery = new EmptyQuery(null);

        return new SelectQuery(null, emptyQuery, selectItems);
    }

    private static class Visitor
            extends SemanticTreeVisitor
    {
        private final String literalValue;

        public Visitor(String literalValue)
        {
            this.literalValue = literalValue;
        }

        @Override
        public void visitLiteralExpression(LiteralExpression node)
        {
            assertEquals(literalValue, node.getValue().getValue());
        }
    }
}
