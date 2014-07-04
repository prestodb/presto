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

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestDoubleType
        extends AbstractTestType
{
    public TestDoubleType()
    {
        super(Double.class,
                DOUBLE.createBlockBuilder(new BlockBuilderStatus())
                        .appendDouble(11.11)
                        .appendDouble(11.11)
                        .appendDouble(11.11)
                        .appendDouble(22.22)
                        .appendDouble(22.22)
                        .appendDouble(22.22)
                        .appendDouble(22.22)
                        .appendDouble(22.22)
                        .appendDouble(33.33)
                        .appendDouble(33.33)
                        .appendDouble(44.44)
                        .build());
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Double) value) + 0.1;
    }
}
