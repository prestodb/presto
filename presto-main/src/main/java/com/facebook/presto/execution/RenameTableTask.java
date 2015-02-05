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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.RenameTable;

import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;

public class RenameTableTask
        implements DataDefinitionTask<RenameTable>
{
    @Override
    public String getName()
    {
        return "RENAME TABLE";
    }

    @Override
    public void execute(RenameTable statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement.getSource());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        QualifiedTableName target = createQualifiedTableName(session, statement.getTarget());
        if (!metadata.getCatalogNames().containsKey(target.getCatalogName())) {
            throw new SemanticException(MISSING_CATALOG, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.getTableHandle(session, target).isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Target table '%s' already exists", target);
        }

        metadata.renameTable(tableHandle.get(), target);
    }
}
