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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.RenameColumn;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class RenameColumnTask
        implements DataDefinitionTask<RenameColumn>
{
    @Override
    public String getName()
    {
        return "RENAME COLUMN";
    }

    @Override
    public void execute(RenameColumn statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement.getTable());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(tableHandle.get());
        if (!columnHandles.containsKey(statement.getSource())) {
            throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", statement.getSource());
        }

        if (columnHandles.containsKey(statement.getTarget())) {
            throw new SemanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", statement.getTarget());
        }
        metadata.renameColumn(tableHandle.get(), columnHandles.get(statement.getSource()), statement.getTarget());
    }
}
