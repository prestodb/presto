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
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_COLUMN_TYPE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN_DEFINITION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class CreateTableTask
        implements DataDefinitionTask<CreateTable>
{
    private void createColumnList(List<ColumnMetadata> columnList, List<TableElement> elementList,
                                    boolean isPartition, int startPos, CreateTable statement)
    {
        int ordinalPosition = startPos;
        Iterator<TableElement> itr = elementList.iterator();
        while (itr.hasNext()) {
            TableElement tblElement = itr.next();
            Type columnType = null;
            switch (tblElement.getDataType().getTypeName()) {
                case BOOLEAN:
                    columnType = BOOLEAN;
                    break;
                case CHAR:
                case VARCHAR:
                    columnType = VARCHAR;
                    break;
                case INTEGER:
                    columnType = BIGINT;
                    break;
                case NUMERIC:
                    columnType = DOUBLE;
                    break;
                case DATE:
                    columnType = DATE;
                    break;
                default:
                    throw new SemanticException(INVALID_COLUMN_TYPE, statement, "Column Type %s not valid", tblElement.getDataType().getTypeName().name());
                }
            ColumnMetadata columnMetadata = new ColumnMetadata(tblElement.getName(), columnType, ordinalPosition, isPartition);
            columnList.add(columnMetadata);
            ordinalPosition = ordinalPosition + 1;
        }
    }

    @Override
    public void execute(CreateTable statement, ConnectorSession session, Metadata metadata)
    {
        QualifiedTableName tableName = createQualifiedTableName(session, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (statement.getIfNotExists().isPresent()) {
            if (tableHandle.isPresent()) {
                return;
            }
        }
        else if (tableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
        }

        if (statement.getTableElementList().isEmpty()) {
            throw new SemanticException(MISSING_COLUMN_DEFINITION, statement, "Table '%s' does not have column definition", tableName);
        }

        List<ColumnMetadata> createTableColumnList = new ArrayList<ColumnMetadata>();
        createColumnList(createTableColumnList,
                            statement.getTableElementList(),
                            false,
                            0,
                            statement);
        createColumnList(createTableColumnList,
                            statement.getPartitionElementList(),
                            true,
                            createTableColumnList.size(),
                            statement);

        ConnectorTableMetadata connectorTableMeta = new ConnectorTableMetadata(
                                                        tableName.asSchemaTableName(),
                                                        createTableColumnList,
                                                        session.getUser(),
                                                        false);

        TableMetadata tableMetadata = new TableMetadata(tableName.getCatalogName(), connectorTableMeta);

        metadata.createTable(session, tableName.getCatalogName(), tableMetadata);
    }
}
