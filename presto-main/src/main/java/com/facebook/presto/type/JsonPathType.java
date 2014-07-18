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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

public class JsonPathType
        implements Type
{
    public static final JsonPathType JSON_PATH = new JsonPathType();

    public static JsonPathType getInstance()
    {
        return JSON_PATH;
    }

    @Override
    public String getName()
    {
        return "JsonPath";
    }

    @Override
    public Class<?> getJavaType()
    {
        return JsonPath.class;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL.toErrorCode(), "JsonPath type cannot be serialized");
    }
}
