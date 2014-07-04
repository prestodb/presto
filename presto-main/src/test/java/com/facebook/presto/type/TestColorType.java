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

import com.facebook.presto.spi.block.BlockBuilderStatus;

import static com.facebook.presto.operator.scalar.ColorFunctions.rgb;
import static com.facebook.presto.type.ColorType.COLOR;

public class TestColorType
        extends AbstractTestType
{
    public TestColorType()
    {
        super(String.class,
                COLOR.createBlockBuilder(new BlockBuilderStatus())
                        .appendLong(rgb(1, 1, 1))
                        .appendLong(rgb(1, 1, 1))
                        .appendLong(rgb(1, 1, 1))
                        .appendLong(rgb(2, 2, 2))
                        .appendLong(rgb(2, 2, 2))
                        .appendLong(rgb(2, 2, 2))
                        .appendLong(rgb(2, 2, 2))
                        .appendLong(rgb(2, 2, 2))
                        .appendLong(rgb(3, 3, 3))
                        .appendLong(rgb(3, 3, 3))
                        .appendLong(rgb(4, 4, 4))
                        .build());
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
