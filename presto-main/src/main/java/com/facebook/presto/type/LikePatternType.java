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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import org.joni.Regex;

public class LikePatternType
        implements Type
{
    public static final LikePatternType LIKE_PATTERN = new LikePatternType();

    public static LikePatternType getInstance()
    {
        return LIKE_PATTERN;
    }

    @Override
    public String getName()
    {
        return "LikePattern";
    }

    @Override
    public Class<?> getJavaType()
    {
        return Regex.class;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL.toErrorCode(), "LikePattern type cannot be serialized");
    }
}
