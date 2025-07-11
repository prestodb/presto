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
import com.facebook.presto.sql.tree.AddConstraint;
import com.facebook.presto.sql.tree.AlterColumnNotNull;
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
import com.facebook.presto.sql.tree.DropConstraint;
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
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.RenameView;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetProperties;
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
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public final class StatementUtils
{
    private StatementUtils() {}

    private static final Map<Class<? extends Statement>, QueryType> STATEMENT_QUERY_TYPES;

    static {
        ImmutableMap.Builder<Class<? extends Statement>, QueryType> builder = ImmutableMap.builder();

        builder.put(Query.class, QueryType.SELECT);

        builder.put(Explain.class, QueryType.EXPLAIN);
        builder.put(Analyze.class, QueryType.ANALYZE);

        builder.put(CreateTableAsSelect.class, QueryType.INSERT);
        builder.put(Insert.class, QueryType.INSERT);
        builder.put(RefreshMaterializedView.class, QueryType.INSERT);

        builder.put(Delete.class, QueryType.DELETE);
        builder.put(Update.class, QueryType.UPDATE);
        builder.put(Merge.class, QueryType.MERGE);

        builder.put(ShowCatalogs.class, QueryType.DESCRIBE);
        builder.put(ShowCreate.class, QueryType.DESCRIBE);
        builder.put(ShowCreateFunction.class, QueryType.DESCRIBE);
        builder.put(ShowFunctions.class, QueryType.DESCRIBE);
        builder.put(ShowGrants.class, QueryType.DESCRIBE);
        builder.put(ShowRoles.class, QueryType.DESCRIBE);
        builder.put(ShowRoleGrants.class, QueryType.DESCRIBE);
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
        builder.put(CreateType.class, QueryType.DATA_DEFINITION);
        builder.put(AddColumn.class, QueryType.DATA_DEFINITION);
        builder.put(CreateTable.class, QueryType.DATA_DEFINITION);
        builder.put(RenameTable.class, QueryType.DATA_DEFINITION);
        builder.put(RenameColumn.class, QueryType.DATA_DEFINITION);
        builder.put(DropColumn.class, QueryType.DATA_DEFINITION);
        builder.put(DropTable.class, QueryType.DATA_DEFINITION);
        builder.put(DropConstraint.class, QueryType.DATA_DEFINITION);
        builder.put(AddConstraint.class, QueryType.DATA_DEFINITION);
        builder.put(AlterColumnNotNull.class, QueryType.DATA_DEFINITION);
        builder.put(CreateView.class, QueryType.DATA_DEFINITION);
        builder.put(RenameView.class, QueryType.DATA_DEFINITION);
        builder.put(TruncateTable.class, QueryType.DATA_DEFINITION);
        builder.put(DropView.class, QueryType.DATA_DEFINITION);
        builder.put(CreateMaterializedView.class, QueryType.DATA_DEFINITION);
        builder.put(DropMaterializedView.class, QueryType.DATA_DEFINITION);
        builder.put(CreateFunction.class, QueryType.CONTROL);
        builder.put(AlterFunction.class, QueryType.DATA_DEFINITION);
        builder.put(DropFunction.class, QueryType.CONTROL);
        builder.put(Use.class, QueryType.CONTROL);
        builder.put(SetSession.class, QueryType.CONTROL);
        builder.put(SetProperties.class, QueryType.DATA_DEFINITION);
        builder.put(ResetSession.class, QueryType.CONTROL);
        builder.put(StartTransaction.class, QueryType.CONTROL);
        builder.put(Commit.class, QueryType.CONTROL);
        builder.put(Rollback.class, QueryType.CONTROL);
        builder.put(Call.class, QueryType.DATA_DEFINITION);
        builder.put(CreateRole.class, QueryType.DATA_DEFINITION);
        builder.put(DropRole.class, QueryType.DATA_DEFINITION);
        builder.put(GrantRoles.class, QueryType.DATA_DEFINITION);
        builder.put(RevokeRoles.class, QueryType.DATA_DEFINITION);
        builder.put(SetRole.class, QueryType.CONTROL);
        builder.put(Grant.class, QueryType.DATA_DEFINITION);
        builder.put(Revoke.class, QueryType.DATA_DEFINITION);
        builder.put(Prepare.class, QueryType.CONTROL);
        builder.put(Deallocate.class, QueryType.CONTROL);

        STATEMENT_QUERY_TYPES = builder.build();
    }

    public static Set<QueryType> getAllQueryTypes()
    {
        return unmodifiableSet(new HashSet<>(STATEMENT_QUERY_TYPES.values()));
    }

    public static Optional<QueryType> getQueryType(Class<? extends Statement> statement)
    {
        return Optional.ofNullable(STATEMENT_QUERY_TYPES.get(statement));
    }

    public static boolean isTransactionControlStatement(Statement statement)
    {
        return statement instanceof StartTransaction || statement instanceof Commit || statement instanceof Rollback;
    }

    public static boolean isRollbackStatement(Statement statement)
    {
        return statement instanceof Rollback;
    }
}
