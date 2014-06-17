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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.CreateTempTable;
import com.facebook.presto.sql.tree.Node;

public class VeroPostgreSqlFormatter
{
    private VeroPostgreSqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    public static class Formatter
        extends VeroGenericSqlFormatter.Formatter
    {
        public Formatter(StringBuilder builder)
        {
            super(builder);
        }

        @Override
        protected Void visitCreateTempTable(CreateTempTable node, Integer indent)
        {
            builder.append("CREATE TEMP TABLE ")
                    .append(node.getName())
                    .append(" AS ");

            process(node.getQuery(), indent);

            builder.append(" WITH DATA");

            return null;
        }
    }
}
