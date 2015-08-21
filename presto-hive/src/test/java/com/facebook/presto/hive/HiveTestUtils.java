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
package com.facebook.presto.hive;

import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.util.List;

import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    public static final ImmutableSet<HivePageSourceFactory> DEFAULT_HIVE_DATA_STREAM_FACTORIES = ImmutableSet.<HivePageSourceFactory>builder()
            .add(new RcFilePageSourceFactory(TYPE_MANAGER))
            .add(new OrcPageSourceFactory(TYPE_MANAGER))
            .add(new DwrfPageSourceFactory(TYPE_MANAGER))
            .build();

    public static final ImmutableSet<HiveRecordCursorProvider> DEFAULT_HIVE_RECORD_CURSOR_PROVIDER = ImmutableSet.<HiveRecordCursorProvider>builder()
            .add(new OrcRecordCursorProvider())
            .add(new ParquetRecordCursorProvider(false))
            .add(new DwrfRecordCursorProvider())
            .add(new ColumnarTextHiveRecordCursorProvider())
            .add(new ColumnarBinaryHiveRecordCursorProvider())
            .add(new GenericHiveRecordCursorProvider())
            .build();

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }

    public static Slice decimalArraySliceOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return arraySliceOf(type, longDecimal);
        }
        else {
            Slice sliceDecimal = LongDecimalType.unscaledValueToSlice(decimal.unscaledValue());
            return arraySliceOf(type, sliceDecimal);
        }
    }

    public static Slice arraySliceOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    public static Slice decimalMapSliceOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return mapSliceOf(type, type, longDecimal, longDecimal);
        }
        else {
            Slice sliceDecimal = LongDecimalType.unscaledValueToSlice(decimal.unscaledValue());
            return mapSliceOf(type, type, sliceDecimal, sliceDecimal);
        }
    }

    public static Slice mapSliceOf(Type keyType, Type valueType, Object key, Object value)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        appendToBlockBuilder(keyType, key, blockBuilder);
        appendToBlockBuilder(valueType, value, blockBuilder);
        return buildStructuralSlice(blockBuilder);
    }

    public static Slice rowSliceOf(List<Type> parameterTypes, Object... values)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }
}
