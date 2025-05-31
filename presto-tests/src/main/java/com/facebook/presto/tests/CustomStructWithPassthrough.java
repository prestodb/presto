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
package com.facebook.presto.tests;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.FunctionDescriptor;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.common.type.StandardTypes.ROW;

public final class CustomStructWithPassthrough
{
    private CustomStructWithPassthrough() {}

    @ScalarFunction(value = "custom_struct_with_passthrough")
    @TypeParameter(value = "T", boundedBy = ROW)
    @FunctionDescriptor(pushdownSubfieldArgIndex = 0)
    @SqlType("T")
    public static Block customStructWithPassthrough(@SqlType("T") Block struct)
    {
        return struct;
    }
}
