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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class DistinctType
        implements Type
{
    private final QualifiedObjectName name;
    // Distinct type definition specifies whether it is orderable or not, even if baseType is orderable.
    private final boolean isOrderable;
    private final Type baseType;

    private final Optional<QualifiedObjectName> parentTypeName;
    private Optional<DistinctType> lazilyLoadedParentType;
    private Function<QualifiedObjectName, DistinctType> distinctTypeLoader;

    public DistinctType(QualifiedObjectName name, Type baseType, boolean isOrderable, List<Optional<QualifiedObjectName>> ancestors, Function<QualifiedObjectName, DistinctType> distinctTypeLoader)
    {
        this(name, baseType, isOrderable, new ArrayDeque<>(ancestors), distinctTypeLoader);
    }

    private DistinctType(QualifiedObjectName name, Type baseType, boolean isOrderable, ArrayDeque<Optional<QualifiedObjectName>> ancestors, Function<QualifiedObjectName, DistinctType> distinctTypeLoader)
    {
        this.name = requireNonNull(name, "name is null");
        this.distinctTypeLoader = requireNonNull(distinctTypeLoader, "distinctTypeLoader is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.isOrderable = isOrderable;
        this.parentTypeName = ancestors.getFirst();
        ancestors.pollFirst();
        if (parentTypeName.isPresent()) {
            this.lazilyLoadedParentType = ancestors.isEmpty() ? null : Optional.of(new DistinctType(name, baseType, isOrderable, ancestors, distinctTypeLoader));
        }
        else {
            this.lazilyLoadedParentType = Optional.empty();
        }
    }

    public QualifiedObjectName getName()
    {
        return name;
    }

    public Type getBaseType()
    {
        return baseType;
    }

    public Optional<DistinctType> getParentTypeLoadIfNeeded()
    {
        assureLoaded();
        return lazilyLoadedParentType;
    }

    public Optional<DistinctType> getParentTypeIfAvailable()
    {
        return lazilyLoadedParentType == null ? Optional.empty() : lazilyLoadedParentType;
    }

    public Optional<QualifiedObjectName> getParentTypeName()
    {
        return parentTypeName;
    }

    public static boolean hasAncestorRelationship(DistinctType firstType, DistinctType secondType)
    {
        Optional<DistinctType> lowestCommonSuperType = getLowestCommonSuperType(firstType, secondType);
        return lowestCommonSuperType.isPresent() && (lowestCommonSuperType.get().equals(firstType) || lowestCommonSuperType.get().equals(secondType));
    }

    // Computes the lowest common ancestor of 2 distinct types in the type tree.
    // Visits as few types as possible, in order to avoid network calls to fetch the types.
    public static Optional<DistinctType> getLowestCommonSuperType(DistinctType firstType, DistinctType secondType)
    {
        if (firstType.getBaseType() != secondType.getBaseType()) {
            return Optional.empty();
        }

        HashMap<QualifiedObjectName, DistinctType> seenTypes = new HashMap<>();
        Optional<DistinctType> firstAncestor = Optional.of(firstType);
        Optional<DistinctType> secondAncestor = Optional.of(secondType);

        seenTypes.put(firstAncestor.get().getName(), firstAncestor.get());
        while (firstAncestor.get().getParentTypeIfAvailable().isPresent()) {
            firstAncestor = firstAncestor.get().getParentTypeIfAvailable();
            seenTypes.put(firstAncestor.get().getName(), firstAncestor.get());
        }

        if (seenTypes.containsKey(secondAncestor.get().getName())) {
            return secondAncestor;
        }
        seenTypes.put(secondAncestor.get().getName(), secondAncestor.get());

        while (secondAncestor.get().getParentTypeIfAvailable().isPresent()) {
            secondAncestor = secondAncestor.get().getParentTypeIfAvailable();
            if (seenTypes.containsKey(secondAncestor.get().getName())) {
                return secondAncestor;
            }
            seenTypes.put(secondAncestor.get().getName(), secondAncestor.get());
        }

        while (firstAncestor.isPresent() || secondAncestor.isPresent()) {
            if (firstAncestor.isPresent() && firstAncestor.get().getParentTypeName().isPresent()) {
                if (seenTypes.containsKey(firstAncestor.get().getParentTypeName().get())) {
                    return Optional.of(seenTypes.get(firstAncestor.get().getParentTypeName().get()));
                }
                firstAncestor = firstAncestor.get().getParentTypeLoadIfNeeded();
                seenTypes.put(firstAncestor.get().getName(), firstAncestor.get());
            }

            if (secondAncestor.isPresent() && secondAncestor.get().getParentTypeName().isPresent()) {
                if (seenTypes.containsKey(secondAncestor.get().getParentTypeName().get())) {
                    return Optional.of(seenTypes.get(secondAncestor.get().getParentTypeName().get()));
                }
                secondAncestor = secondAncestor.get().getParentTypeLoadIfNeeded();
                seenTypes.put(secondAncestor.get().getName(), secondAncestor.get());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DistinctType other = (DistinctType) obj;

        return Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public TypeSignature getTypeSignature()
    {
        List<Optional<QualifiedObjectName>> ancestors = new ArrayList<>();
        DistinctType type = this;
        while (type != null) {
            Optional<QualifiedObjectName> parentTypeName = type.getParentTypeName();
            ancestors.add(parentTypeName);
            type = getParentTypeIfAvailable().orElse(null);
        }
        return new TypeSignature(new DistinctTypeInfo(name, baseType.getTypeSignature(), ancestors, isOrderable));
    }

    @Override
    public String getDisplayName()
    {
        return name.toString();
    }

    @Override
    public boolean isComparable()
    {
        return baseType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return isOrderable;
    }

    @Override
    public Class<?> getJavaType()
    {
        return baseType.getJavaType();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return baseType.getTypeParameters();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return baseType.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return baseType.createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        return baseType.getObjectValue(properties, block, position);
    }

    @Override
    public boolean getBoolean(Block block, int position)
    {
        return baseType.getBoolean(block, position);
    }

    @Override
    public boolean getBooleanUnchecked(UncheckedBlock block, int internalPosition)
    {
        return baseType.getBooleanUnchecked(block, internalPosition);
    }

    @Override
    public long getLong(Block block, int position)
    {
        return baseType.getLong(block, position);
    }

    @Override
    public long getLongUnchecked(UncheckedBlock block, int internalPosition)
    {
        return baseType.getLongUnchecked(block, internalPosition);
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return baseType.getDouble(block, position);
    }

    @Override
    public double getDoubleUnchecked(UncheckedBlock block, int internalPosition)
    {
        return baseType.getDoubleUnchecked(block, internalPosition);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return baseType.getSlice(block, position);
    }

    @Override
    public Slice getSliceUnchecked(Block block, int internalPosition)
    {
        return baseType.getSliceUnchecked(block, internalPosition);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        return baseType.getObject(block, position);
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        return baseType.getBlockUnchecked(block, internalPosition);
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        baseType.writeBoolean(blockBuilder, value);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        baseType.writeLong(blockBuilder, value);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        baseType.writeDouble(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        baseType.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        baseType.writeSlice(blockBuilder, value, offset, length);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        baseType.writeObject(blockBuilder, value);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        baseType.appendTo(block, position, blockBuilder);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return baseType.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public long hash(Block block, int position)
    {
        return baseType.hash(block, position);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return baseType.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    private void assureLoaded()
    {
        if (lazilyLoadedParentType != null) {
            return;
        }
        lazilyLoadedParentType = Optional.of(distinctTypeLoader.apply(parentTypeName.get()));
        distinctTypeLoader = null;
    }
}
