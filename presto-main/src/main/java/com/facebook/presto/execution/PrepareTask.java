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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.inject.Inject;

import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PrepareTask
        implements DataDefinitionTask<Prepare>
{
    private final SqlParser sqlParser;

    @Inject
    public PrepareTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "PREPARE";
    }

    @Override
    public String explain(Prepare statement)
    {
        return "PREPARE " + statement.getName();
    }

    @Override
    public CompletableFuture<?> execute(Prepare prepare, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        Statement statement = prepare.getStatement();
        if ((statement instanceof Prepare) || (statement instanceof Execute) || (statement instanceof Deallocate)) {
            String type = statement.getClass().getSimpleName().toUpperCase(ENGLISH);
            throw new PrestoException(NOT_SUPPORTED, "Invalid statement type for prepared statement: " + type);
        }

        String sql = getFormattedSql(statement, sqlParser);
        stateMachine.addPreparedStatement(prepare.getName(), sql);
        return completedFuture(null);
    }
}
