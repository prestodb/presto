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
package com.facebook.presto.execution;

import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;

import java.util.Optional;

import static com.facebook.presto.spi.resourceGroups.QueryType.DELETE;
import static com.facebook.presto.spi.resourceGroups.QueryType.DESCRIBE;
import static com.facebook.presto.spi.resourceGroups.QueryType.EXPLAIN;
import static com.facebook.presto.spi.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.spi.resourceGroups.QueryType.SELECT;
import static java.util.Objects.requireNonNull;

public abstract class AbstractQueryExecution
        implements QueryExecution
{
    final Statement statement;

    AbstractQueryExecution(Statement statement)
    {
        this.statement = requireNonNull(statement, "statement is null");
    }

    @Override
    public Optional<QueryType> getQueryType()
    {
        return getSqlQueryType(statement);
    }

    public static Optional<QueryType> getSqlQueryType(Statement statement)
    {
        if (statement instanceof Query) {
            return Optional.of(SELECT);
        }
        else if (statement instanceof Explain) {
            return Optional.of(EXPLAIN);
        }
        else if (statement instanceof ShowCatalogs || statement instanceof ShowCreate || statement instanceof ShowFunctions ||
                statement instanceof ShowGrants || statement instanceof ShowPartitions || statement instanceof ShowSchemas ||
                statement instanceof ShowSession || statement instanceof ShowStats || statement instanceof ShowTables ||
                statement instanceof ShowColumns || statement instanceof DescribeInput || statement instanceof DescribeOutput) {
            return Optional.of(DESCRIBE);
        }
        else if (statement instanceof CreateTableAsSelect || statement instanceof Insert) {
            return Optional.of(INSERT);
        }
        else if (statement instanceof Delete) {
            return Optional.of(DELETE);
        }
        return Optional.empty();
    }
}
