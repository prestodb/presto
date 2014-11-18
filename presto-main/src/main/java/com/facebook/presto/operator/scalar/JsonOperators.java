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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.type.JsonType.JSON;

public class JsonOperators
{
    private JsonOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equals(@SqlType(StandardTypes.JSON) Slice leftJson, @SqlType(StandardTypes.JSON) Slice rightJson)
    {
        BlockBuilder leftBlockBuilder = JSON.createBlockBuilder(new BlockBuilderStatus());
        BlockBuilder rightBlockBuilder = JSON.createBlockBuilder(new BlockBuilderStatus());
        leftBlockBuilder.writeBytes(leftJson, 0, leftJson.length());
        rightBlockBuilder.writeBytes(rightJson, 0, rightJson.length());
        return JSON.equalTo(leftBlockBuilder.closeEntry().build(), 0, rightBlockBuilder.closeEntry().build(), 0);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.JSON) Slice leftJson, @SqlType(StandardTypes.JSON) Slice rightJson)
    {
        return !equals(leftJson, rightJson);
    }
}
