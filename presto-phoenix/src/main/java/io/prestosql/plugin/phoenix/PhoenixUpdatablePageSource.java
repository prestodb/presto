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
package io.prestosql.plugin.phoenix;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.getFullTableName;
import static io.prestosql.plugin.phoenix.TypeUtils.getObjectValue;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

public class PhoenixUpdatablePageSource
        extends PhoenixPageSource
        implements UpdatablePageSource
{
    private PhoenixSplit split;
    private PhoenixConnection deleteConn;
    private PreparedStatement deleteStatement;
    private List<Field> pkFields;

    public PhoenixUpdatablePageSource(PhoenixClient phoenixClient, PhoenixSplit split,
            List<PhoenixColumnHandle> columns)
    {
        super(phoenixClient, split, columns);
        this.split = split;
        RowType pkRowType = (RowType) PhoenixMetadata.getPkHandle(columns).get().getColumnType();
        this.pkFields = pkRowType.getFields();
        try {
            String deleteSql = buildDeleteSql();
            this.deleteConn = phoenixClient.getConnection();
            this.deleteConn.setAutoCommit(true); // executed server side if true
            this.deleteStatement = deleteConn.prepareStatement(deleteSql);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        try {
            for (int i = 0; i < rowIds.getPositionCount(); i++) {
                Block singleRowBlock = rowIds.getObject(i, Block.class);
                for (int fieldPos = 0; fieldPos < pkFields.size(); fieldPos++) {
                    Field field = pkFields.get(fieldPos);
                    Object value = getObjectValue(field.getType(), singleRowBlock, fieldPos);
                    if (value instanceof Array) {
                        deleteStatement.setArray(fieldPos + 1, (Array) value);
                    }
                    else {
                        deleteStatement.setObject(fieldPos + 1, value);
                    }
                }
                deleteStatement.addBatch();
            }
            deleteStatement.executeBatch();
            deleteConn.commit();
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    private String buildDeleteSql()
    {
        String columns = pkFields.stream().map(field -> field.getName().get()).collect(joining(","));
        String vars = Joiner.on(",").join(nCopies(pkFields.size(), "?"));
        StringBuilder deleteSql = new StringBuilder()
                .append("DELETE FROM ")
                .append(getFullTableName(split.getCatalogName(), split.getSchemaName(), split.getTableName()))
                .append(" WHERE ")
                .append("(").append(columns).append(")")
                .append(" IN ")
                .append("((").append(vars).append("))");
        return deleteSql.toString();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // close resources
        try (PhoenixConnection connection = this.deleteConn;
                PreparedStatement statement = this.deleteStatement) {
            // do nothing
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }
}
