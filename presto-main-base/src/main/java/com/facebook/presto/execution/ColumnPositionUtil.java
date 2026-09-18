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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ColumnPosition;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.ColumnPosition.After;
import com.facebook.presto.sql.tree.ColumnPosition.First;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Statement;

import java.util.Map;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

/**
 * The {@code FIRST | AFTER <column>} clause is shared by {@code ADD COLUMN} and {@code ALTER COLUMN},
 * so both statements resolve it the same way.
 */
final class ColumnPositionUtil
{
    private ColumnPositionUtil() {}

    /**
     * Converts a parsed position clause into the connector representation, normalizing the {@code AFTER}
     * target the same way any other column reference is normalized and rejecting a target that is not a
     * usable column of the table.
     * <p>
     * The two {@code ColumnPosition} types in scope here are distinct: the unqualified {@code First} and
     * {@code After} are {@link com.facebook.presto.sql.tree.ColumnPosition} from the statement, while
     * {@code ColumnPosition.First} and the like are the {@link ColumnPosition} handed to the connector.
     */
    static ColumnPosition toConnectorColumnPosition(
            com.facebook.presto.sql.tree.ColumnPosition position,
            Statement statement,
            Metadata metadata,
            Session session,
            String catalogName,
            TableHandle tableHandle,
            Map<String, ColumnHandle> columnHandles)
    {
        if (position instanceof First) {
            return new ColumnPosition.First();
        }
        if (position instanceof After) {
            Identifier afterIdentifier = ((After) position).getColumn();
            String afterColumn = metadata.normalizeIdentifier(session, catalogName, afterIdentifier.getValue());
            ColumnHandle afterColumnHandle = columnHandles.get(afterColumn);
            if (afterColumnHandle == null) {
                throw new SemanticException(MISSING_COLUMN, statement, "Column '%s' does not exist", afterIdentifier.getValue());
            }
            // A hidden column, such as a connector's synthesized "$path", has no place in the table's column
            // order, so nothing can be positioned after it even though getColumnHandles exposes it
            if (metadata.getColumnMetadata(session, tableHandle, afterColumnHandle).isHidden()) {
                throw new SemanticException(NOT_SUPPORTED, statement, "Cannot position a column after hidden column '%s'", afterIdentifier.getValue());
            }
            return new ColumnPosition.After(afterColumn);
        }
        throw new SemanticException(NOT_SUPPORTED, statement, "Unsupported column position: %s", position);
    }
}
