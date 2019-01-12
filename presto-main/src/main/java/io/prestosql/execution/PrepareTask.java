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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.Execute;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.SqlFormatterUtil.getFormattedSql;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

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
    public String explain(Prepare statement, List<Expression> parameters)
    {
        return "PREPARE " + statement.getName();
    }

    @Override
    public ListenableFuture<?> execute(Prepare prepare, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Statement statement = prepare.getStatement();
        if ((statement instanceof Prepare) || (statement instanceof Execute) || (statement instanceof Deallocate)) {
            String type = statement.getClass().getSimpleName().toUpperCase(ENGLISH);
            throw new PrestoException(NOT_SUPPORTED, "Invalid statement type for prepared statement: " + type);
        }

        String sql = getFormattedSql(statement, sqlParser, Optional.empty());
        stateMachine.addPreparedStatement(prepare.getName().getValue(), sql);
        return immediateFuture(null);
    }
}
