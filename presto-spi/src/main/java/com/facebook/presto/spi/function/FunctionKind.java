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
package com.facebook.presto.spi.function;

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.presto.common.experimental.auto_gen.ThriftFunctionKind;

@ThriftEnum
public enum FunctionKind
{
    SCALAR(1),
    AGGREGATE(2),
    WINDOW(3);

    private final int value;

    public static FunctionKind createFunctionKind(ThriftFunctionKind thriftFunctionKind)
    {
        return FunctionKind.valueOf(thriftFunctionKind.name());
    }

    public ThriftFunctionKind toThrift()
    {
        return ThriftFunctionKind.valueOf(this.name());
    }

    FunctionKind(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
