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
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.transaction.TransactionManager;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class PrepareTask
        implements DataDefinitionTask<Prepare>
{
  @Override
  public String explain(Prepare statement)
  {
    return "PREPARE " + statement.getIdentifier();
  }

  @Override
  public String getName()
  {
    return "PREPARE";
  }

  @Override
  public CompletableFuture<?> execute(Prepare statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
  {
    String statementName = statement.getIdentifier();
    String queryString = SqlFormatter.formatSql(statement.getQuery());
    stateMachine.addPreparedStatement(statementName, queryString);
    return completedFuture(null);
  }
}
