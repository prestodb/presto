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

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.ColumnPosition;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * A rewriter that renames column references has to reach the identifier of an {@code AFTER <column>}
 * position too, otherwise the rewritten statement positions the new column against a name that no
 * longer exists in the rewritten context.
 */
public class TestDefaultTreeRewriterAddColumn
{
    @Test
    public void testAfterIdentifierIsRewritten()
    {
        AddColumn rewritten = (AddColumn) new ColumnRenamer()
                .process(addColumn(Optional.of(new ColumnPosition.After(new Identifier("old_name")))), null);

        Optional<ColumnPosition> position = rewritten.getPosition();
        assertTrue(position.isPresent() && position.get() instanceof ColumnPosition.After);
        assertEquals(((ColumnPosition.After) position.get()).getColumn(), new Identifier("new_name"));
    }

    @Test
    public void testUnchangedNodeIsReturnedAsIs()
    {
        // The early return must not fire when only the position needs rewriting, and must still fire
        // when nothing does
        AddColumn unaffected = addColumn(Optional.of(new ColumnPosition.After(new Identifier("untouched"))));
        assertSame(new ColumnRenamer().process(unaffected, null), unaffected);

        AddColumn noPosition = addColumn(Optional.empty());
        assertSame(new ColumnRenamer().process(noPosition, null), noPosition);
    }

    @Test
    public void testPositionsWithoutIdentifierArePreserved()
    {
        assertEquals(rewrittenPosition(new ColumnPosition.First()), Optional.of(new ColumnPosition.First()));
    }

    private static Optional<ColumnPosition> rewrittenPosition(ColumnPosition position)
    {
        return ((AddColumn) new ColumnRenamer().process(addColumn(Optional.of(position)), null)).getPosition();
    }

    private static AddColumn addColumn(Optional<ColumnPosition> position)
    {
        ColumnDefinition column = new ColumnDefinition(new Identifier("new_column"), "INTEGER", true, emptyList(), Optional.empty());
        return new AddColumn(QualifiedName.of("test_table"), column, position, false, false);
    }

    /**
     * Renames the single column {@code old_name} to {@code new_name}, standing in for any subclass that
     * rewrites column references. Expression reconstruction is left to subclasses, so this supplies the
     * {@code visitExpression} implementation the base class requires.
     */
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
