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

import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateType;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropRole;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowCreateFunction;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowRoleGrants;
import com.facebook.presto.sql.tree.ShowRoles;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.TruncateTable;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class StatementUtils
{
    private StatementUtils() {}

    private static final Map<String, QueryType> STATEMENT_QUERY_TYPES;
    private static final List<String> SESSION_TRANSACTION_CONTROL_TYPES;

    static {
        ImmutableMap.Builder<String, QueryType> builder = ImmutableMap.builder();
        ImmutableList.Builder<String> sessionTransactionBuilder = ImmutableList.builder();

        builder.put(Query.class.getSimpleName(), QueryType.SELECT);

        builder.put(Explain.class.getSimpleName(), QueryType.EXPLAIN);
        builder.put(Analyze.class.getSimpleName(), QueryType.ANALYZE);

        builder.put(CreateTableAsSelect.class.getSimpleName(), QueryType.INSERT);
        builder.put(Insert.class.getSimpleName(), QueryType.INSERT);
        builder.put(RefreshMaterializedView.class.getSimpleName(), QueryType.INSERT);

        builder.put(Delete.class.getSimpleName(), QueryType.DELETE);

        builder.put(ShowCatalogs.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowCreate.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowCreateFunction.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowFunctions.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowGrants.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowRoles.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowRoleGrants.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowSchemas.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowSession.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowStats.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowTables.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(ShowColumns.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(DescribeInput.class.getSimpleName(), QueryType.DESCRIBE);
        builder.put(DescribeOutput.class.getSimpleName(), QueryType.DESCRIBE);

        builder.put(CreateSchema.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropSchema.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(RenameSchema.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateType.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(AddColumn.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateTable.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(RenameTable.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(RenameColumn.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropColumn.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropTable.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateView.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(TruncateTable.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropView.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateMaterializedView.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropMaterializedView.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateFunction.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(AlterFunction.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropFunction.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Use.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(SetSession.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(ResetSession.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(StartTransaction.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Commit.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Rollback.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Call.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(CreateRole.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(DropRole.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(GrantRoles.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(RevokeRoles.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(SetRole.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Grant.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Revoke.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Prepare.class.getSimpleName(), QueryType.DATA_DEFINITION);
        builder.put(Deallocate.class.getSimpleName(), QueryType.DATA_DEFINITION);

        sessionTransactionBuilder.add(Use.class.getSimpleName());
        sessionTransactionBuilder.add(SetSession.class.getSimpleName());
        sessionTransactionBuilder.add(ResetSession.class.getSimpleName());
        sessionTransactionBuilder.add(SetRole.class.getSimpleName());
        sessionTransactionBuilder.add(StartTransaction.class.getSimpleName());
        sessionTransactionBuilder.add(Commit.class.getSimpleName());
        sessionTransactionBuilder.add(Rollback.class.getSimpleName());
        sessionTransactionBuilder.add(Prepare.class.getSimpleName());
        sessionTransactionBuilder.add(Deallocate.class.getSimpleName());

        STATEMENT_QUERY_TYPES = builder.build();
        SESSION_TRANSACTION_CONTROL_TYPES = sessionTransactionBuilder.build();
    }

    public static Map<String, QueryType> getAllQueryTypes()
    {
        return STATEMENT_QUERY_TYPES;
    }

    public static Optional<QueryType> getQueryType(String statement)
    {
        return Optional.ofNullable(STATEMENT_QUERY_TYPES.get(statement));
    }

    public static boolean isTransactionControlStatement(Statement statement)
    {
        return statement instanceof StartTransaction || statement instanceof Commit || statement instanceof Rollback;
    }

    public static boolean isSessionTransactionControlStatement(String statement)
    {
        return SESSION_TRANSACTION_CONTROL_TYPES.contains(statement);
    }
}
