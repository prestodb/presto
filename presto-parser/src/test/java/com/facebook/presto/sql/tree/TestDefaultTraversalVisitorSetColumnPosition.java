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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

/**
 * {@link DefaultTraversalVisitor} enumerates the children of {@link SetColumnPosition} explicitly rather
 * than iterating {@link Node#getChildren()}, so both the moved column and the identifier of an
 * {@code AFTER <column>} position have to be visited deliberately. An analysis pass built on the visitor
 * that collects column references would otherwise silently skip them.
 */
public class TestDefaultTraversalVisitorSetColumnPosition
{
    @Test
    public void testMovedColumnAndAfterIdentifierAreVisited()
    {
        assertEquals(
                visitedIdentifiers(setColumnPosition(new ColumnPosition.After(new Identifier("existing_column")))),
                ImmutableList.of("moved_column", "existing_column"));
    }

    @Test
    public void testFirstVisitsOnlyMovedColumn()
    {
        assertEquals(visitedIdentifiers(setColumnPosition(new ColumnPosition.First())), ImmutableList.of("moved_column"));
    }

    private static SetColumnPosition setColumnPosition(ColumnPosition position)
    {
        return new SetColumnPosition(QualifiedName.of("test_table"), new Identifier("moved_column"), position, false);
    }

    private static List<String> visitedIdentifiers(SetColumnPosition node)
    {
        IdentifierCollector collector = new IdentifierCollector();
        collector.process(node, null);
        return collector.getIdentifiers().stream()
                .map(Identifier::getValue)
                .collect(toImmutableList());
    }

    private static class IdentifierCollector
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final ImmutableList.Builder<Identifier> identifiers = ImmutableList.builder();

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            identifiers.add(node);
            return null;
        }

        public List<Identifier> getIdentifiers()
        {
            return identifiers.build();
        }
    }
}
