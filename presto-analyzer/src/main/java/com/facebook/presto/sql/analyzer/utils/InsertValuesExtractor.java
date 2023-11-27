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
package com.facebook.presto.sql.analyzer.utils;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Values;

import java.util.ArrayList;
import java.util.List;

public class InsertValuesExtractor
{
    private InsertValuesExtractor() {}

    public static InsertValuesMessage getInsertValuesMessage(Statement statement)
    {
        InsertValuesExtractingVisitor insertValuesExtractingVisitor = new InsertValuesExtractingVisitor();
        insertValuesExtractingVisitor.process(statement, null);
        return insertValuesExtractingVisitor.getInsertValuesMessage();
    }

    public static class InsertValuesMessage
    {
        private final boolean insertValues;
        private final int rowsCount;

        public InsertValuesMessage(boolean insertValues, int rowsCount)
        {
            this.insertValues = insertValues;
            this.rowsCount = rowsCount;
        }

        public boolean isInsertValues()
        {
            return insertValues;
        }

        public int getRowsCount()
        {
            return rowsCount;
        }
    }

    private static class InsertValuesExtractingVisitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        private final List<Expression> rows = new ArrayList<>();
        boolean hasInsert;
        boolean hasSelect;

        public InsertValuesMessage getInsertValuesMessage()
        {
            boolean insertValues = hasInsert && !hasSelect && !rows.isEmpty();
            return new InsertValuesMessage(insertValues, insertValues ? rows.size() : -1);
        }

        @Override
        protected Void visitInsert(Insert node, Void context)
        {
            hasInsert = true;
            process(node.getQuery(), context);
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Void context)
        {
            // there exists 'select' in this statement, stop handling
            hasSelect = true;
            return null;
        }

        @Override
        public Void visitValues(Values node, Void context)
        {
            if (hasInsert && !hasSelect) {
                rows.addAll(node.getRows());
            }
            return null;
        }
    }
}
