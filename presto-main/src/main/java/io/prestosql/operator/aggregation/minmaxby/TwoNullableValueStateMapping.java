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
package io.prestosql.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class TwoNullableValueStateMapping
{
    private TwoNullableValueStateMapping() {}

    private static final Map<List<Class<?>>, Class<? extends AccumulatorState>> STATE_MAPPINGS;

    static {
        STATE_MAPPINGS = new ImmutableMap.Builder<List<Class<?>>, Class<? extends AccumulatorState>>()
                .put(ImmutableList.of(boolean.class, Block.class), BooleanAndBlockPositionValueState.class)
                .put(ImmutableList.of(boolean.class, boolean.class), BooleanBooleanState.class)
                .put(ImmutableList.of(boolean.class, double.class), BooleanDoubleState.class)
                .put(ImmutableList.of(boolean.class, long.class), BooleanLongState.class)
                .put(ImmutableList.of(boolean.class, Slice.class), BooleanAndBlockPositionValueState.class)
                .put(ImmutableList.of(double.class, boolean.class), DoubleBooleanState.class)
                .put(ImmutableList.of(double.class, Block.class), DoubleAndBlockPositionValueState.class)
                .put(ImmutableList.of(double.class, double.class), DoubleDoubleState.class)
                .put(ImmutableList.of(double.class, long.class), DoubleLongState.class)
                .put(ImmutableList.of(double.class, Slice.class), DoubleAndBlockPositionValueState.class)
                .put(ImmutableList.of(long.class, Block.class), LongAndBlockPositionValueState.class)
                .put(ImmutableList.of(long.class, boolean.class), LongBooleanState.class)
                .put(ImmutableList.of(long.class, double.class), LongDoubleState.class)
                .put(ImmutableList.of(long.class, long.class), LongLongState.class)
                .put(ImmutableList.of(long.class, Slice.class), LongAndBlockPositionValueState.class)
                .put(ImmutableList.of(Slice.class, boolean.class), SliceBooleanState.class)
                .put(ImmutableList.of(Slice.class, Block.class), SliceAndBlockPositionValueState.class)
                .put(ImmutableList.of(Slice.class, double.class), SliceDoubleState.class)
                .put(ImmutableList.of(Slice.class, long.class), SliceLongState.class)
                .put(ImmutableList.of(Slice.class, Slice.class), SliceAndBlockPositionValueState.class)
                .put(ImmutableList.of(Block.class, boolean.class), BlockBooleanState.class)
                .put(ImmutableList.of(Block.class, Block.class), BlockAndBlockPositionValueState.class)
                .put(ImmutableList.of(Block.class, double.class), BlockDoubleState.class)
                .put(ImmutableList.of(Block.class, long.class), BlockLongState.class)
                .put(ImmutableList.of(Block.class, Slice.class), BlockAndBlockPositionValueState.class)
                .build();
    }

    public static Class<? extends AccumulatorState> getStateClass(Class<?> first, Class<?> second)
    {
        List<Class<?>> key = ImmutableList.of(first, second);
        Class<? extends AccumulatorState> state = STATE_MAPPINGS.get(key);
        checkArgument(state != null, "Unsupported state type combination: (%s, %s)", first.getName(), second.getName());
        return state;
    }

    public static AccumulatorStateSerializer<?> getStateSerializer(Type firstType, Type secondType)
    {
        Class<?> firstJavaType = firstType.getJavaType();
        Class<?> secondJavaType = secondType.getJavaType();

        if (secondJavaType != Block.class && secondJavaType != Slice.class) {
            throw new IllegalArgumentException(format("Unsupported state type combination: (%s, %s)", firstJavaType.getName(), secondJavaType.getName()));
        }
        if (firstJavaType == boolean.class) {
            return new BooleanAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == long.class) {
            return new LongAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == double.class) {
            return new DoubleAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == Slice.class) {
            return new SliceAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == Block.class) {
            return new BlockAndBlockPositionStateSerializer(firstType, secondType);
        }
        throw new IllegalArgumentException(format("Unsupported state type combination: (%s, %s)", firstJavaType.getName(), secondJavaType.getName()));
    }
}
