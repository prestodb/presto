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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.sql.tree.ColumnPosition;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetColumnPosition;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestDefaultTreeRewriterSetColumnPosition
{
    @Test
    public void testMovedColumnIsRewritten()
    {
        SetColumnPosition rewritten = (SetColumnPosition) new ColumnRenamer()
                .process(setColumnPosition(new Identifier("old_name"), new ColumnPosition.First()), null);

        assertEquals(rewritten.getColumn(), new Identifier("new_name"));
    }

    @Test
    public void testAfterIdentifierIsRewritten()
    {
        SetColumnPosition rewritten = (SetColumnPosition) new ColumnRenamer()
                .process(setColumnPosition(new Identifier("col"), new ColumnPosition.After(new Identifier("old_name"))), null);

        ColumnPosition position = rewritten.getPosition();
        assertTrue(position instanceof ColumnPosition.After);
        assertEquals(((ColumnPosition.After) position).getColumn(), new Identifier("new_name"));
    }

    @Test
    public void testUnchangedNodeIsReturnedAsIs()
    {
        SetColumnPosition unaffected = setColumnPosition(new Identifier("col"), new ColumnPosition.After(new Identifier("untouched")));
        assertSame(new ColumnRenamer().process(unaffected, null), unaffected);

        SetColumnPosition firstPosition = setColumnPosition(new Identifier("col"), new ColumnPosition.First());
        assertSame(new ColumnRenamer().process(firstPosition, null), firstPosition);
    }

    @Test
    public void testFirstPositionIsPreserved()
    {
        SetColumnPosition rewritten = (SetColumnPosition) new ColumnRenamer()
                .process(setColumnPosition(new Identifier("col"), new ColumnPosition.First()), null);

        assertTrue(rewritten.getPosition() instanceof ColumnPosition.First);
    }

    private static SetColumnPosition setColumnPosition(Identifier column, ColumnPosition position)
    {
        return new SetColumnPosition(QualifiedName.of("test_table"), column, position, false);
    }

    private static class ColumnRenamer
            extends DefaultTreeRewriter<Void>
    {
        @Override
        protected Node visitExpression(Expression node, Void context)
        {
            if ((node instanceof Identifier) && ((Identifier) node).getValue().equals("old_name")) {
                return new Identifier("new_name");
            }
            return node;
        }
    }
}
