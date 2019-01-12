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
package io.prestosql.type;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.Map;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.StructuralTestUtil.mapBlockOf;
import static io.prestosql.util.StructuralTestUtil.mapType;

public class TestIntegerVarcharMapType
        extends AbstractTestType
{
    public TestIntegerVarcharMapType()
    {
        super(mapType(INTEGER, VARCHAR), Map.class, createTestBlock(mapType(INTEGER, VARCHAR)));
    }

    public static Block createTestBlock(Type mapType)
    {
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 2);
        mapType.writeObject(blockBuilder, mapBlockOf(INTEGER, VARCHAR, ImmutableMap.of(1, "hi")));
        mapType.writeObject(blockBuilder, mapBlockOf(INTEGER, VARCHAR, ImmutableMap.of(1, "2", 2, "hello")));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }
}
