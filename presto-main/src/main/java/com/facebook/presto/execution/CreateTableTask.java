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
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;

public class CreateTableTask
        implements DataDefinitionTask<CreateTable>
{
    @Override
    public String getName()
    {
        return "CREATE TABLE";
    }

    @Override
    public void execute(CreateTable statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");

        QualifiedTableName tableName = createQualifiedTableName(session, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
        }

        List<ColumnMetadata> columns = new ArrayList<>();
        int ordinalPosition = 0;
        for (TableElement element : statement.getElements()) {
            Type type = metadata.getType(parseTypeSignature(element.getType()));
            if ((type == null) || type.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, element, "Unknown type for column '%s' ", element.getName());
            }
            columns.add(new ColumnMetadata(element.getName(), type, ordinalPosition, false));
            ordinalPosition++;
        }

        TableMetadata tableMetadata = new TableMetadata(
                tableName.getCatalogName(),
                new ConnectorTableMetadata(tableName.asSchemaTableName(), columns, session.getUser(), false));

        metadata.createTable(session, tableName.getCatalogName(), tableMetadata);
    }
}
