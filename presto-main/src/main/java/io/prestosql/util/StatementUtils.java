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
package io.prestosql.util;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.Call;
import io.prestosql.sql.tree.Commit;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.DescribeInput;
import io.prestosql.sql.tree.DescribeOutput;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.sql.tree.SetPath;
import io.prestosql.sql.tree.SetSession;
import io.prestosql.sql.tree.ShowCatalogs;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.sql.tree.ShowCreate;
import io.prestosql.sql.tree.ShowFunctions;
import io.prestosql.sql.tree.ShowGrants;
import io.prestosql.sql.tree.ShowSchemas;
import io.prestosql.sql.tree.ShowSession;
import io.prestosql.sql.tree.ShowStats;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Use;

import java.util.Map;
import java.util.Optional;

public final class StatementUtils
{
    private StatementUtils() {}

    private static final Map<Class<? extends Statement>, QueryType> STATEMENT_QUERY_TYPES;

    static {
        ImmutableMap.Builder<Class<? extends Statement>, QueryType> builder = ImmutableMap.builder();
        builder.put(Query.class, QueryType.SELECT);

        builder.put(Explain.class, QueryType.EXPLAIN);

        builder.put(CreateTableAsSelect.class, QueryType.INSERT);
        builder.put(Insert.class, QueryType.INSERT);

        builder.put(Delete.class, QueryType.DELETE);

        builder.put(ShowCatalogs.class, QueryType.DESCRIBE);
        builder.put(ShowCreate.class, QueryType.DESCRIBE);
        builder.put(ShowFunctions.class, QueryType.DESCRIBE);
        builder.put(ShowGrants.class, QueryType.DESCRIBE);
        builder.put(ShowSchemas.class, QueryType.DESCRIBE);
        builder.put(ShowSession.class, QueryType.DESCRIBE);
        builder.put(ShowStats.class, QueryType.DESCRIBE);
        builder.put(ShowTables.class, QueryType.DESCRIBE);
        builder.put(ShowColumns.class, QueryType.DESCRIBE);
        builder.put(DescribeInput.class, QueryType.DESCRIBE);
        builder.put(DescribeOutput.class, QueryType.DESCRIBE);

        builder.put(CreateSchema.class, QueryType.DATA_DEFINITION);
        builder.put(DropSchema.class, QueryType.DATA_DEFINITION);
        builder.put(RenameSchema.class, QueryType.DATA_DEFINITION);
        builder.put(AddColumn.class, QueryType.DATA_DEFINITION);
        builder.put(CreateTable.class, QueryType.DATA_DEFINITION);
        builder.put(RenameTable.class, QueryType.DATA_DEFINITION);
        builder.put(RenameColumn.class, QueryType.DATA_DEFINITION);
        builder.put(DropColumn.class, QueryType.DATA_DEFINITION);
        builder.put(DropTable.class, QueryType.DATA_DEFINITION);
        builder.put(CreateView.class, QueryType.DATA_DEFINITION);
        builder.put(DropView.class, QueryType.DATA_DEFINITION);
        builder.put(Use.class, QueryType.DATA_DEFINITION);
        builder.put(SetSession.class, QueryType.DATA_DEFINITION);
        builder.put(ResetSession.class, QueryType.DATA_DEFINITION);
        builder.put(StartTransaction.class, QueryType.DATA_DEFINITION);
        builder.put(Commit.class, QueryType.DATA_DEFINITION);
        builder.put(Rollback.class, QueryType.DATA_DEFINITION);
        builder.put(Call.class, QueryType.DATA_DEFINITION);
        builder.put(Grant.class, QueryType.DATA_DEFINITION);
        builder.put(Revoke.class, QueryType.DATA_DEFINITION);
        builder.put(Prepare.class, QueryType.DATA_DEFINITION);
        builder.put(Deallocate.class, QueryType.DATA_DEFINITION);
        builder.put(SetPath.class, QueryType.DATA_DEFINITION);
        STATEMENT_QUERY_TYPES = builder.build();
    }

    public static Map<Class<? extends Statement>, QueryType> getAllQueryTypes()
    {
        return STATEMENT_QUERY_TYPES;
    }

    public static Optional<QueryType> getQueryType(Class<? extends Statement> statement)
    {
        return Optional.ofNullable(STATEMENT_QUERY_TYPES.get(statement));
    }

    public static boolean isTransactionControlStatement(Statement statement)
    {
        return statement instanceof StartTransaction || statement instanceof Commit || statement instanceof Rollback;
    }
}
