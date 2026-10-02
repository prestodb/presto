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
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

/**
 * {@link DefaultTraversalVisitor} enumerates the children of {@link AddColumn} explicitly rather than
 * iterating {@link Node#getChildren()}, so the identifier of an {@code AFTER <column>} position has to
 * be visited deliberately. An analysis pass built on the visitor that collects column references
 * would otherwise silently skip the position target.
 */
public class TestDefaultTraversalVisitorAddColumn
{
    @Test
    public void testAfterIdentifierIsVisited()
    {
        Identifier afterColumn = new Identifier("existing_column");

        assertEquals(
                visitedIdentifiers(addColumn(Optional.of(new ColumnPosition.After(afterColumn)))),
                ImmutableList.of("existing_column"));
    }

    @Test
    public void testPositionsWithoutIdentifierVisitNothing()
    {
        // The visitor does not descend into a ColumnDefinition, so the AFTER target is the only
        // identifier reachable from an AddColumn node
        assertEquals(visitedIdentifiers(addColumn(Optional.empty())), ImmutableList.of());
        assertEquals(visitedIdentifiers(addColumn(Optional.of(new ColumnPosition.First()))), ImmutableList.of());
    }

    private static AddColumn addColumn(Optional<ColumnPosition> position)
    {
        ColumnDefinition column = new ColumnDefinition(new Identifier("new_column"), "INTEGER", true, emptyList(), Optional.empty());
        return new AddColumn(QualifiedName.of("test_table"), column, position, false, false);
    }

    private static List<String> visitedIdentifiers(AddColumn node)
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
