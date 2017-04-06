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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class TypedSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final int SET_INSTANCE_SIZE = ClassLayout.parseClass(ObjectOpenHashSet.class).instanceSize();
    private static final int ELEMENT_REFERENCE_INSTANCE_SIZE = ClassLayout.parseClass(ElementReference.class).instanceSize();
    private static final DataSize MAX_SET_SIZE = new DataSize(4, MEGABYTE);

    private final Type elementType;
    private final Set<ElementReference> set;

    public TypedSet(Type elementType, int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize is negative");
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.set = new ObjectOpenHashSet<>(expectedSize);
    }

    public int size()
    {
        return set.size();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getRetainedSizeOfSetInBytes();
    }

    private long getRetainedSizeOfSetInBytes()
    {
        return SET_INSTANCE_SIZE + set.size() * ELEMENT_REFERENCE_INSTANCE_SIZE;
    }

    public Optional<ElementReference> find(ElementReference elementReference)
    {
        requireNonNull(elementReference, "elementReference is null");
        for (ElementReference existing : set) {
            if (existing.equals(elementReference)) {
                return Optional.of(existing);
            }
        }
        return Optional.empty();
    }

    public boolean contains(Block block, int position)
    {
        requireNonNull(block, "block is null");
        checkArgument(position >= 0, "position is negative");
        return set.contains(ElementReference.of(block, elementType, position));
    }

    public void add(Block block, int position)
    {
        requireNonNull(block, "block is null");
        checkArgument(position >= 0, "position is negative");
        set.add(ElementReference.of(block, elementType, position));
        if (getRetainedSizeOfSetInBytes() > MAX_SET_SIZE.toBytes()) {
            throw exceededLocalLimit(MAX_SET_SIZE);
        }
    }

    public static class ElementReference
    {
        private final Block block;
        private final Type type;
        private final int position;

        public ElementReference(Block block, Type type, int position)
        {
            this.block = requireNonNull(block, "block is null");
            this.type = requireNonNull(type, "type is null");
            this.position = position;
        }

        public Block getBlock()
        {
            return block;
        }

        public Type getType()
        {
            return type;
        }

        public int getPosition()
        {
            return position;
        }

        public static ElementReference of(Block block, Type type, int position)
        {
            return new ElementReference(block, type, position);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ElementReference other = (ElementReference) o;
            if (!other.type.equals(type)) {
                return false;
            }
            return positionEqualsPosition(type, block, position, other.block, other.position);
        }

        @Override
        public int hashCode()
        {
            return (int) murmurHash3(hashPosition(type, block, position));
        }
    }
}
