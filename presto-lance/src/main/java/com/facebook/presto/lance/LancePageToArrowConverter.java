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
package com.facebook.presto.lance;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static java.lang.Float.intBitsToFloat;

public final class LancePageToArrowConverter
{
    private LancePageToArrowConverter() {}

    public static Schema toArrowSchema(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            ArrowType arrowType = LanceColumnHandle.toArrowType(column.getType());
            fields.add(new Field(column.getName(), new FieldType(column.isNullable(), arrowType, null), null));
        }
        return new Schema(fields.build());
    }

    public static void writeBlockToVector(Block block, FieldVector vector, Type type, int rowCount)
    {
        writeBlockToVectorAtOffset(block, vector, type, rowCount, 0);
    }

    public static void writeBlockToVectorAtOffset(Block block, FieldVector vector, Type type, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                // Arrow vectors handle nulls automatically with null bitmap
                continue;
            }
            int targetIndex = offset + i;
            if (type instanceof BooleanType) {
                ((BitVector) vector).setSafe(targetIndex, type.getBoolean(block, i) ? 1 : 0);
            }
            else if (type instanceof TinyintType) {
                ((TinyIntVector) vector).setSafe(targetIndex, (byte) type.getLong(block, i));
            }
            else if (type instanceof SmallintType) {
                ((SmallIntVector) vector).setSafe(targetIndex, (short) type.getLong(block, i));
            }
            else if (type instanceof IntegerType) {
                ((IntVector) vector).setSafe(targetIndex, (int) type.getLong(block, i));
            }
            else if (type instanceof BigintType) {
                ((BigIntVector) vector).setSafe(targetIndex, type.getLong(block, i));
            }
            else if (type instanceof RealType) {
                ((Float4Vector) vector).setSafe(targetIndex, intBitsToFloat((int) type.getLong(block, i)));
            }
            else if (type instanceof DoubleType) {
                ((Float8Vector) vector).setSafe(targetIndex, type.getDouble(block, i));
            }
            else if (type instanceof VarcharType) {
                byte[] bytes = type.getSlice(block, i).getBytes();
                ((VarCharVector) vector).setSafe(targetIndex, bytes);
            }
            else if (type instanceof VarbinaryType) {
                byte[] bytes = type.getSlice(block, i).getBytes();
                ((VarBinaryVector) vector).setSafe(targetIndex, bytes);
            }
            else if (type instanceof DateType) {
                ((DateDayVector) vector).setSafe(targetIndex, (int) type.getLong(block, i));
            }
            else if (type instanceof TimestampType) {
                ((TimeStampMicroVector) vector).setSafe(targetIndex, type.getLong(block, i));
            }
            else {
                throw new PrestoException(LanceErrorCode.LANCE_TYPE_NOT_SUPPORTED,
                        "Unsupported type for Arrow conversion: " + type);
            }
        }
    }
}
