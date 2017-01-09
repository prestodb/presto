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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.TypeSignature;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

public class PostgreSqlJsonType extends AbstractVariableWidthType
{
    public static final PostgreSqlJsonType POSTGRESQL_JSON = new PostgreSqlJsonType(false);
    public static final PostgreSqlJsonType POSTGRESQL_JSONB = new PostgreSqlJsonType(true);

    private boolean isJsonB;

    private PostgreSqlJsonType(boolean isJsonB)
    {
        super(TypeSignature.parseTypeSignature(isJsonB ? "pg_jsonb" : "pg_json"), Object.class);
        this.isJsonB = isJsonB;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        PGobject leftObject = getObject(leftBlock, leftPosition);
        PGobject rightObject = getObject(rightBlock, rightPosition);
        return leftObject == null ? rightObject == null : leftObject.equals(rightObject);
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, block.getLength(position));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return getObject(block, position).getValue();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public PGobject getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getLength(position));
        PGobject pGobject = new PGobject();
        pGobject.setType(isJsonB ? "jsonb" : "json");
        try {
            pGobject.setValue(slice.toStringUtf8());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return pGobject;
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (!(value instanceof PGobject)) {
            throw new RuntimeException("Tyring to create PostgreSQL Json Type from non-PGobject");
        }
        Slice slice = Slices.utf8Slice(((PGobject) value).getValue());
        blockBuilder.writeBytes(slice, 0, slice.length());
        blockBuilder.closeEntry();
    }
}
