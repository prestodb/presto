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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowType
        extends AbstractVariableWidthType
{
    private final List<RowField> fields;

    public RowType(List<Type> fieldTypes, List<String> fieldNames)
    {
        super(new TypeSignature("row", Lists.transform(fieldTypes, Type::getTypeSignature), ImmutableList.<Object>copyOf(fieldNames)), Slice.class);
        ImmutableList.Builder<RowField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            builder.add(new RowField(fieldTypes.get(i), fieldNames.get(i)));
        }
        fields = builder.build();
    }

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < this.fields.size(); i++) {
            fields.add(this.fields.get(i).getName() + " " + this.fields.get(i).getType().getDisplayName());
        }
        return "ROW(" + Joiner.on(", ").join(fields) + ")";
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getLength(position));
        return stackRepresentationToObject(session, slice, this);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(blockBuilderStatus);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return FluentIterable.from(fields).transform(new Function<RowField, Type>() {
            @Override
            public Type apply(RowField input)
            {
                return input.getType();
            }
        }).toList();
    }

    public List<RowField> getFields()
    {
        return fields;
    }

    public static class RowField
    {
        private final Type type;
        private final String name;

        public RowField(Type type, String name)
        {
            this.type = checkNotNull(type, "type is null");
            this.name = checkNotNull(name, "name is null");
        }

        public Type getType()
        {
            return type;
        }

        public String getName()
        {
            return name;
        }
    }
}
