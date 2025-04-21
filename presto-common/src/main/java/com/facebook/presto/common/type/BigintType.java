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
import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftBigintType;
import com.facebook.presto.common.experimental.auto_gen.ThriftType;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public final class BigintType
        extends AbstractLongType
{
    public static final BigintType BIGINT = new BigintType();

    static {
        ThriftSerializationRegistry.registerSerializer(BigintType.class, BigintType::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(BigintType.class, null, BigintType::singletonDeseriaize, null);
    }

    public BigintType(ThriftBigintType thriftBigintType)
    {
        this();
    }

    @Override
    public ThriftBigintType toThrift()
    {
        return new ThriftBigintType();
    }

    @Override
    public ThriftType toThriftInterface()
    {
        return ThriftType.builder()
                .setType(getImplementationType())
                .setSerializedData(FbThriftUtils.serialize(this.toThrift()))
                .build();
    }

    private BigintType()
    {
        super(parseTypeSignature(StandardTypes.BIGINT));
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
        return other == BIGINT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    public static BigintType singletonDeseriaize(byte[] bytes)
    {
        return BIGINT;
    }
}
