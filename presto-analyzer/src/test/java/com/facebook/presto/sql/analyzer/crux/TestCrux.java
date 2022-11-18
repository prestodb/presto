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
package com.facebook.presto.sql.analyzer.crux;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCrux
{
    @Test
    public void testSimpleQuery()
    {
        String query = "SELECT 10";
        SemanticTree tree = Crux.sqlToTree(query);

        assertTrue(tree.isStatement());
        assertTrue(tree.asStatement().isQuery());
        assertTrue(tree.asStatement().asQuery().isSelect());

        Visitor visitor = new Visitor();
        visitor.visitSemanticTree(tree);
    }

    @Test
    public void testEmptyQuery()
    {
        String query = "";
        SemanticTree tree = Crux.sqlToTree(query);

        assertTrue(tree.isStatement());
        assertTrue(tree.asStatement().isQuery());
        assertTrue(tree.asStatement().asQuery().isEmpty());
    }

    private static class Visitor
            extends SemanticTreeVisitor
    {
        @Override
        public void visitLiteralExpression(LiteralExpression node)
        {
            assertEquals(10, node.getValue());
        }
    }
}
