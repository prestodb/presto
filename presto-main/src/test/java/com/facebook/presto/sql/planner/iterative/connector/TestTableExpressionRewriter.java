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

package com.facebook.presto.sql.planner.iterative.connector;

import com.facebook.presto.spi.relation.BinaryTableExpression;
import com.facebook.presto.spi.relation.LeafTableExpression;
import com.facebook.presto.spi.relation.TableExpression;
import com.facebook.presto.spi.relation.UnaryTableExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.sql.planner.iterative.connector.rewriter.TableExpressionRewriter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestTableExpressionRewriter
{
    @Test
    public void testSimpleRewriter()
    {
        TableExpressionRewriter<String, String> rewriter = new TableExpressionRewriter<String, String>()
                .addRule(TableScan.class, (tableScan, quote) -> quote + "tableScan" + quote)
                .addRule(Project.class, (my, project, quote) -> {
                    String result = my.accept(project.getSource(), quote);
                    return format("%s%s -> %s", quote + "project" + quote, project.getExtra(), result);
                })
                .addRule(UnaryTableExpression.class, (my, unary, quote) -> {
                    String result = my.accept(unary.getSource(), quote);
                    return format("%s -> %s", quote + "unaryNode" + quote, result);
                });
        assertEquals(rewriter.accept(new Project(new Filter(new TableScan())), "`"), "`project`() -> `unaryNode` -> `tableScan`");
    }

    @Test
    public void testRewriterUnknownNode()
    {
        TableExpressionRewriter<String, String> rewriter = new TableExpressionRewriter<String, String>()
                .addRule(TableScan.class, (tableScan, quote) -> quote + "tableScan" + quote)
                .addRule(Project.class, (my, project, quote) -> {
                    String result = my.accept(project.getSource(), quote);
                    return format("%s%s -> %s", quote + "project" + quote, project.getExtra(), result);
                })
                .addRule(UnaryTableExpression.class, (my, unary, quote) -> {
                    String result = my.accept(unary.getSource(), quote);
                    return format("%s -> %s", quote + "unaryNode" + quote, result);
                });
        assertThrows(UnsupportedOperationException.class, () -> rewriter.accept(new Join(new TableScan(), new Project(new Filter(new TableScan()))), "`"));
    }

    private static final class TableScan
            extends LeafTableExpression
    {
        public static TableScan tableScan()
        {
            return new TableScan();
        }

        @Override
        public List<ColumnExpression> getOutput()
        {
            return ImmutableList.of();
        }
    }

    private static final class Project
            extends UnaryTableExpression
    {
        private final TableExpression source;

        public Project(TableExpression source)
        {
            this.source = source;
        }

        public static Project project(TableExpression source)
        {
            return new Project(source);
        }

        public String getExtra()
        {
            return "()";
        }

        @Override
        public List<ColumnExpression> getOutput()
        {
            return ImmutableList.of();
        }

        @Override
        public TableExpression getSource()
        {
            return source;
        }
    }

    private static final class Filter
            extends UnaryTableExpression
    {
        private final TableExpression source;

        public Filter(TableExpression source)
        {
            this.source = source;
        }

        public static Filter filter(TableExpression source)
        {
            return new Filter(source);
        }

        @Override
        public List<ColumnExpression> getOutput()
        {
            return ImmutableList.of();
        }

        @Override
        public TableExpression getSource()
        {
            return source;
        }
    }

    private static final class Join
            extends BinaryTableExpression
    {
        private final TableExpression left;
        private final TableExpression right;

        public Join(TableExpression left, TableExpression right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public List<ColumnExpression> getOutput()
        {
            return null;
        }

        @Override
        public List<TableExpression> getSources()
        {
            return ImmutableList.of(left, right);
        }
    }
}
