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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.AccumulatorState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class TwoNullableValueStateMapping
{
    private TwoNullableValueStateMapping() {}

    private static final Map<List<Class<?>>, Class<? extends AccumulatorState>> MAPPINGS;

    static {
        MAPPINGS = new ImmutableMap.Builder<List<Class<?>>, Class<? extends AccumulatorState>>()
                .put(ImmutableList.of(boolean.class, Block.class), BooleanBlockState.class)
                .put(ImmutableList.of(boolean.class, boolean.class), BooleanBooleanState.class)
                .put(ImmutableList.of(boolean.class, double.class), BooleanDoubleState.class)
                .put(ImmutableList.of(boolean.class, long.class), BooleanLongState.class)
                .put(ImmutableList.of(boolean.class, Slice.class), BooleanSliceState.class)
                .put(ImmutableList.of(boolean.class, void.class), BooleanUnknownState.class)
                .put(ImmutableList.of(double.class, boolean.class), DoubleBooleanState.class)
                .put(ImmutableList.of(double.class, Block.class), DoubleBlockState.class)
                .put(ImmutableList.of(double.class, double.class), DoubleDoubleState.class)
                .put(ImmutableList.of(double.class, long.class), DoubleLongState.class)
                .put(ImmutableList.of(double.class, Slice.class), DoubleSliceState.class)
                .put(ImmutableList.of(double.class, void.class), DoubleUnknownState.class)
                .put(ImmutableList.of(long.class, Block.class), LongBlockState.class)
                .put(ImmutableList.of(long.class, boolean.class), LongBooleanState.class)
                .put(ImmutableList.of(long.class, double.class), LongDoubleState.class)
                .put(ImmutableList.of(long.class, long.class), LongLongState.class)
                .put(ImmutableList.of(long.class, Slice.class), LongSliceState.class)
                .put(ImmutableList.of(long.class, void.class), LongUnknownState.class)
                .put(ImmutableList.of(Slice.class, boolean.class), SliceBooleanState.class)
                .put(ImmutableList.of(Slice.class, Block.class), SliceBlockState.class)
                .put(ImmutableList.of(Slice.class, double.class), SliceDoubleState.class)
                .put(ImmutableList.of(Slice.class, long.class), SliceLongState.class)
                .put(ImmutableList.of(Slice.class, Slice.class), SliceSliceState.class)
                .put(ImmutableList.of(Slice.class, void.class), SliceUnknownState.class)
                .put(ImmutableList.of(Block.class, boolean.class), BlockBooleanState.class)
                .put(ImmutableList.of(Block.class, Block.class), BlockBlockState.class)
                .put(ImmutableList.of(Block.class, double.class), BlockDoubleState.class)
                .put(ImmutableList.of(Block.class, long.class), BlockLongState.class)
                .put(ImmutableList.of(Block.class, Slice.class), BlockSliceState.class)
                .put(ImmutableList.of(Block.class, void.class), BlockUnknownState.class)
                .put(ImmutableList.of(void.class, void.class), TwoNullableValueState.class)
                .put(ImmutableList.of(void.class, Block.class), UnknownBlockState.class)
                .put(ImmutableList.of(void.class, boolean.class), UnknownBooleanState.class)
                .put(ImmutableList.of(void.class, double.class), UnknownDoubleState.class)
                .put(ImmutableList.of(void.class, long.class), UnknownLongState.class)
                .put(ImmutableList.of(void.class, Slice.class), UnknownSliceState.class)
                .build();
    }

    public static Class<? extends AccumulatorState> getStateClass(Class<?> first, Class<?> second)
    {
        List<Class<?>> key = ImmutableList.of(first, second);
        Class<? extends AccumulatorState> state = MAPPINGS.get(key);
        checkArgument(state != null, "Unsupported state type combination: (%s, %s)", first.getName(), second.getName());
        return state;
    }
}
