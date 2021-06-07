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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;

public class ExtractTableNameVisitor<R, C>
        extends AstVisitor<R, C>
{
    public ExtractTableNameVisitor()
    {
        super();
    }

    @Override
    protected R visitQuery(Query node, C context)
    {
        return (R) process(node.getQueryBody());
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
        if (node.getFrom().isPresent()){
            return (R) process(node.getFrom().get());
        }
        return null;
    }

    @Override
    protected R visitTable(Table node, C context)
    {
        return (R) node.getName();
    }
}
