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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.spi.relation.RowExpression;

import java.util.Comparator;

public class RowExpressionComparator
        implements Comparator<RowExpression>
{
    RowExpressionEquivalenceVisitor rowExpressionEquivalenceVisitor = new RowExpressionEquivalenceVisitor();

    @Override
    public int compare(RowExpression o1, RowExpression o2)
    {
        if (o1.accept(rowExpressionEquivalenceVisitor, o2)) {
            return 0;
        }
        else if (o1.hashCode() > o2.hashCode()) {
            return 1; // This exists so that when this comparator is used in treeMap, tree remains a balanced tree.
        }
        return -1;
    }
}
