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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public class TestingIdType
        extends AbstractLongType
{
    public static final TestingIdType ID = new TestingIdType();
    public static final String NAME = "id";

    private TestingIdType()
    {
        super(parseTypeSignature(NAME));
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getLong(position);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == ID;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
