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
package com.facebook.presto.util;

import com.facebook.presto.execution.AddColumnTask;
import com.facebook.presto.execution.AlterFunctionTask;
import com.facebook.presto.execution.CallTask;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateFunctionTask;
import com.facebook.presto.execution.CreateMaterializedViewTask;
import com.facebook.presto.execution.CreateRoleTask;
import com.facebook.presto.execution.CreateSchemaTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateTypeTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DropColumnTask;
import com.facebook.presto.execution.DropFunctionTask;
import com.facebook.presto.execution.DropMaterializedViewTask;
import com.facebook.presto.execution.DropRoleTask;
import com.facebook.presto.execution.DropSchemaTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.GrantRolesTask;
import com.facebook.presto.execution.GrantTask;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameSchemaTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RevokeRolesTask;
import com.facebook.presto.execution.RevokeTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetRoleTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.UseTask;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateType;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropMaterializedView;
import com.facebook.presto.sql.tree.DropRole;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

import java.util.Map;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

/**
 * Helper class for binding Statement to  DataDefinitionTask
 */
public class PrestoDataDefBindingHelper
{
    private PrestoDataDefBindingHelper(){}

    private static final Map<Class<? extends Statement>, Class<? extends DataDefinitionTask<?>>> STATEMENT_TASK_TYPES;
    private static final Map<Class<? extends Statement>, Class<? extends DataDefinitionTask<?>>> TRANSACTION_CONTROL_TYPES;

    static {
        ImmutableMap.Builder<Class<? extends Statement>, Class<? extends DataDefinitionTask<?>>> dataDefBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Class<? extends Statement>, Class<? extends DataDefinitionTask<?>>> transactionDefBuilder = ImmutableMap.builder();
        dataDefBuilder.put(CreateSchema.class, CreateSchemaTask.class);
        dataDefBuilder.put(DropSchema.class, DropSchemaTask.class);
        dataDefBuilder.put(RenameSchema.class, RenameSchemaTask.class);
        dataDefBuilder.put(CreateType.class, CreateTypeTask.class);
        dataDefBuilder.put(AddColumn.class, AddColumnTask.class);
        dataDefBuilder.put(CreateTable.class, CreateTableTask.class);
        dataDefBuilder.put(RenameTable.class, RenameTableTask.class);
        dataDefBuilder.put(RenameColumn.class, RenameColumnTask.class);
        dataDefBuilder.put(DropColumn.class, DropColumnTask.class);
        dataDefBuilder.put(DropTable.class, DropTableTask.class);
        dataDefBuilder.put(CreateView.class, CreateViewTask.class);
        dataDefBuilder.put(DropView.class, DropViewTask.class);
        dataDefBuilder.put(CreateMaterializedView.class, CreateMaterializedViewTask.class);
        dataDefBuilder.put(DropMaterializedView.class, DropMaterializedViewTask.class);
        dataDefBuilder.put(CreateFunction.class, CreateFunctionTask.class);
        dataDefBuilder.put(AlterFunction.class, AlterFunctionTask.class);
        dataDefBuilder.put(DropFunction.class, DropFunctionTask.class);
        dataDefBuilder.put(Call.class, CallTask.class);
        dataDefBuilder.put(CreateRole.class, CreateRoleTask.class);
        dataDefBuilder.put(DropRole.class, DropRoleTask.class);
        dataDefBuilder.put(GrantRoles.class, GrantRolesTask.class);
        dataDefBuilder.put(RevokeRoles.class, RevokeRolesTask.class);
        dataDefBuilder.put(Grant.class, GrantTask.class);
        dataDefBuilder.put(Revoke.class, RevokeTask.class);

        transactionDefBuilder.put(Use.class, UseTask.class);
        transactionDefBuilder.put(SetSession.class, SetSessionTask.class);
        transactionDefBuilder.put(ResetSession.class, ResetSessionTask.class);
        transactionDefBuilder.put(StartTransaction.class, StartTransactionTask.class);
        transactionDefBuilder.put(Commit.class, CommitTask.class);
        transactionDefBuilder.put(Rollback.class, RollbackTask.class);
        transactionDefBuilder.put(SetRole.class, SetRoleTask.class);
        transactionDefBuilder.put(Prepare.class, PrepareTask.class);
        transactionDefBuilder.put(Deallocate.class, DeallocateTask.class);

        STATEMENT_TASK_TYPES = dataDefBuilder.build();
        TRANSACTION_CONTROL_TYPES = transactionDefBuilder.build();
    }

    public static void bindDDLDefinitionTasks(Binder binder)
    {
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

        STATEMENT_TASK_TYPES.entrySet().stream()
                .forEach(entry -> taskBinder.addBinding(entry.getKey()).to(entry.getValue()).in(Scopes.SINGLETON));
    }

    public static void bindTransactionControlDefinitionTasks(Binder binder)
    {
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {
                }, new TypeLiteral<DataDefinitionTask<?>>() {
                });

        TRANSACTION_CONTROL_TYPES.entrySet().stream()
                .forEach(entry -> taskBinder.addBinding(entry.getKey()).to(entry.getValue()).in(Scopes.SINGLETON));
    }
}
