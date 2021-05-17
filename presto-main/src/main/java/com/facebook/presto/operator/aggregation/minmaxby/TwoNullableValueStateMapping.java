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
package com.facebook.presto.operator.aggregation.minmaxby;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class TwoNullableValueStateMapping
{
    private TwoNullableValueStateMapping() {}

    private static final Map<List<Class<?>>, Class<? extends AccumulatorState>> STATE_MAPPINGS;

    static {
        STATE_MAPPINGS = new ImmutableMap.Builder<List<Class<?>>, Class<? extends AccumulatorState>>()
                .put(ImmutableList.of(boolean.class, boolean.class), BooleanBooleanState.class)
                .put(ImmutableList.of(boolean.class, double.class), BooleanDoubleState.class)
                .put(ImmutableList.of(boolean.class, long.class), BooleanLongState.class)
                .put(ImmutableList.of(boolean.class, Object.class), BooleanAndBlockPositionValueState.class)
                .put(ImmutableList.of(double.class, boolean.class), DoubleBooleanState.class)
                .put(ImmutableList.of(double.class, double.class), DoubleDoubleState.class)
                .put(ImmutableList.of(double.class, long.class), DoubleLongState.class)
                .put(ImmutableList.of(double.class, Object.class), DoubleAndBlockPositionValueState.class)
                .put(ImmutableList.of(long.class, boolean.class), LongBooleanState.class)
                .put(ImmutableList.of(long.class, double.class), LongDoubleState.class)
                .put(ImmutableList.of(long.class, long.class), LongLongState.class)
                .put(ImmutableList.of(long.class, Object.class), LongAndBlockPositionValueState.class)
                .put(ImmutableList.of(Object.class, boolean.class), ObjectBooleanState.class)
                .put(ImmutableList.of(Object.class, double.class), ObjectDoubleState.class)
                .put(ImmutableList.of(Object.class, long.class), ObjectLongState.class)
                .put(ImmutableList.of(Object.class, Object.class), ObjectAndBlockPositionValueState.class)
                .build();
    }

    public static Class<? extends AccumulatorState> getStateClass(Class<?> first, Class<?> second)
    {
        List<Class<?>> key = ImmutableList.of(
                first.isPrimitive() ? first : Object.class,
                second.isPrimitive() ? second : Object.class);
        Class<? extends AccumulatorState> state = STATE_MAPPINGS.get(key);
        checkArgument(state != null, "Unsupported state type combination: (%s, %s)", first.getName(), second.getName());
        return state;
    }

    public static AccumulatorStateSerializer<?> getStateSerializer(Type firstType, Type secondType)
    {
        Class<?> firstJavaType = firstType.getJavaType();
        if (firstJavaType == boolean.class) {
            return new BooleanAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == long.class) {
            return new LongAndBlockPositionStateSerializer(firstType, secondType);
        }
        if (firstJavaType == double.class) {
            return new DoubleAndBlockPositionStateSerializer(firstType, secondType);
        }
        return new ObjectAndBlockPositionStateSerializer(firstType, secondType);
    }
}
