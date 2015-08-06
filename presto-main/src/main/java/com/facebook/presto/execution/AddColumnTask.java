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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.TableElement;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public class AddColumnTask
        implements DataDefinitionTask<AddColumn>
{
    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public void execute(AddColumn statement, Session session, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        accessControl.checkCanAddColumns(session.getIdentity(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        TableElement element = statement.getColumn();
        Type type = metadata.getType(parseTypeSignature(element.getType()));
        if ((type == null) || type.equals(UNKNOWN)) {
            throw new SemanticException(TYPE_MISMATCH, element, "Unknown type for column '%s' ", element.getName());
        }
        if (columnHandles.containsKey(element.getName())) {
            throw new SemanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", element.getName());
        }

        metadata.addColumn(session, tableHandle.get(), new ColumnMetadata(element.getName(), type, false));
    }
}
