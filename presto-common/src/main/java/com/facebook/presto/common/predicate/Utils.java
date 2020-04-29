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
package com.facebook.presto.common.predicate;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;

import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;

public final class Utils
{
    private Utils()
    {
    }

    public static Block nativeValueToBlock(Type type, Object object)
    {
        if (object != null && !Primitives.wrap(type.getJavaType()).isInstance(object)) {
            throw new IllegalArgumentException(String.format("Object '%s' does not match type %s", object, type.getJavaType()));
        }
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, object);
        return blockBuilder.build();
    }

    public static Object blockToNativeValue(Type type, Block block)
    {
        return readNativeValue(type, block, 0);
    }
}
