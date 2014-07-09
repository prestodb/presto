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

import com.google.common.base.Optional;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AlterTable;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

public class AlterTableTask
        implements DataDefinitionTask<AlterTable>
{
    @Override
    public void execute(AlterTable statement, ConnectorSession session, Metadata metadata)
    {
        if (statement.getType() == AlterTable.FunctionType.RENAME) {
            QualifiedTableName tableName = createQualifiedTableName(session, statement.getSource());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
            if (!tableHandle.isPresent()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            QualifiedTableName targetName = createQualifiedTableName(session, statement.getTarget());
            metadata.alterTableRename(tableHandle.get(), tableName.getSchemaName(), tableName.getTableName(), targetName.getSchemaName(), targetName.getTableName());
        }
        else {
            throw new SemanticException(NOT_SUPPORTED, statement, "Operation Not Supported: ", statement.getType());
        }
    }
}
