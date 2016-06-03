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
package com.facebook.presto.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.spi.block.SliceArrayBlock.deepCopyAndCompact;
import static com.facebook.presto.spi.block.SliceArrayBlock.getSliceArrayRetainedSizeInBytes;
import static com.facebook.presto.spi.block.SliceArrayBlock.getSliceArraySizeInBytes;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.copyOf;
import static io.airlift.slice.Slices.wrappedIntArray;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LazySliceArrayBlock
        extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazySliceArrayBlock.class).instanceSize();

    private final int positionCount;
    private final AtomicInteger sizeInBytes = new AtomicInteger(-1);
    private final AtomicInteger retainedSizeInBytes = new AtomicInteger(-1);

    private LazyBlockLoader<LazySliceArrayBlock> loader;
    private Slice[] values;
    private boolean dictionary;
    private int[] ids;
    private boolean[] isNull;

    public LazySliceArrayBlock(int positionCount, LazyBlockLoader<LazySliceArrayBlock> loader)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;
        this.loader = requireNonNull(loader);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new LazySliceArrayBlockEncoding();
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);
        assureLoaded();

        if (dictionary) {
            return compactAndGet(positions, false);
        }

        Slice[] newValues = new Slice[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            if (!isEntryNull(positions.get(i))) {
                newValues[i] = copyOf(values[positions.get(i)]);
            }
        }
        return new SliceArrayBlock(positions.size(), newValues);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        assureLoaded();
        return values[getPosition(position)];
    }

    @Override
    protected int getPositionOffset(int position)
    {
        return 0;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        assureLoaded();
        if (isNull != null) {
            return isNull[position];
        }
        return values[getPosition(position)] == null;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getLength(int position)
    {
        assureLoaded();
        return values[getPosition(position)].length();
    }

    @Override
    public int getSizeInBytes()
    {
        int sizeInBytes = this.sizeInBytes.get();
        if (sizeInBytes < 0) {
            assureLoaded();
            sizeInBytes = getSliceArraySizeInBytes(values);
            if (dictionary) {
                sizeInBytes += ids.length * SIZE_OF_INT;
                sizeInBytes += isNull.length * SIZE_OF_BYTE;
            }
            this.sizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        // TODO: This should account for memory used by the loader.
        int sizeInBytes = this.retainedSizeInBytes.get();
        if (sizeInBytes < 0) {
            assureLoaded();
            sizeInBytes = INSTANCE_SIZE + getSliceArrayRetainedSizeInBytes(values);
            if (dictionary) {
                sizeInBytes += SizeOf.sizeOf(ids);
                sizeInBytes += SizeOf.sizeOf(isNull);
            }
            this.retainedSizeInBytes.set(sizeInBytes);
        }
        return sizeInBytes;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        assureLoaded();

        if (dictionary) {
            List<Integer> positions = IntStream.range(positionOffset, positionOffset + length).boxed().collect(toList());
            return compactAndGet(positions, false);
        }
        Slice[] newValues = Arrays.copyOfRange(values, positionOffset, positionOffset + length);
        return new SliceArrayBlock(length, newValues);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }

        assureLoaded();

        if (dictionary) {
            List<Integer> positions = IntStream.range(positionOffset, positionOffset + length).boxed().collect(toList());
            return compactAndGet(positions, true);
        }
        return new SliceArrayBlock(length, deepCopyAndCompact(values, positionOffset, length));
    }

    @Override
    public void assureLoaded()
    {
        if (values != null) {
            return;
        }
        loader.load(this);

        if (values == null) {
            throw new IllegalArgumentException("Lazy block loader did not load this block");
        }

        // clear reference to loader to free resources, since load was successful
        loader = null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LazySliceArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private Block compactAndGet(List<Integer> positions, boolean copy)
    {
        int[] newIds = new int[positions.size()];
        boolean hasNull = false;

        int[] newDictionaryIndexes = new int[values.length];
        Arrays.fill(newDictionaryIndexes, -1);

        int nextIndex = 0;
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            if (isEntryNull(position)) {
                hasNull = true;
                newIds[i] = -1;
            }
            else {
                int oldIndex = ids[position];
                if (newDictionaryIndexes[oldIndex] == -1) {
                    newDictionaryIndexes[oldIndex] = nextIndex;
                    nextIndex++;
                }
                newIds[i] = newDictionaryIndexes[oldIndex];
            }
        }

        int newDictionaryLength = nextIndex;
        if (hasNull) {
            newDictionaryLength++;
        }
        Slice[] newValues = new Slice[newDictionaryLength];

        for (int i = 0; i < values.length; i++) {
            if (newDictionaryIndexes[i] != -1) {
                // key was referenced
                Slice value = values[i];
                int newIndex = newDictionaryIndexes[i];
                newValues[newIndex] = copy ? copyOf(value) : value;
            }
        }

        if (hasNull) {
            int nullIndex = newValues.length - 1;
            for (int i = 0; i < newIds.length; i++) {
                if (newIds[i] == -1) {
                    newIds[i] = nullIndex;
                }
            }
        }
        return new DictionaryBlock(positions.size(), new SliceArrayBlock(newValues.length, newValues), wrappedIntArray(newIds));
    }

    private int getPosition(int position)
    {
        if (dictionary) {
            return ids[position];
        }
        return position;
    }

    public Slice[] getValues()
    {
        assureLoaded();
        return values;
    }

    public int[] getIds()
    {
        return ids;
    }

    public DictionaryBlock createDictionaryBlock()
    {
        if (!dictionary) {
            throw new IllegalStateException("cannot create dictionary block");
        }
        // if nulls are encoded in values, we can create a new dictionary block
        if (isNull == null) {
            return new DictionaryBlock(getPositionCount(), new SliceArrayBlock(values.length, values), wrappedIntArray(ids));
        }

        boolean hasNulls = false;
        int[] newIds = Arrays.copyOf(ids, ids.length);
        for (int position = 0; position < positionCount; position++) {
            if (isEntryNull(position)) {
                hasNulls = true;
                newIds[position] = values.length;
            }
        }

        // if we found a null, create a new values array with null at the end
        Slice[] newValues = hasNulls ? Arrays.copyOf(values, values.length + 1) : values;
        return new DictionaryBlock(getPositionCount(), new SliceArrayBlock(newValues.length, newValues), wrappedIntArray(newIds));
    }

    public boolean isDictionary()
    {
        assureLoaded();
        return dictionary;
    }

    public void setValues(Slice[] values)
    {
        requireNonNull(values, "values is null");
        this.values = values;
    }

    public void setValues(Slice[] values, int[] ids, boolean[] isNull)
    {
        requireNonNull(values, "values is null");
        requireNonNull(ids, "ids is null");
        requireNonNull(isNull, "isNull is null");

        if (ids.length != positionCount) {
            throw new IllegalArgumentException(format("ids length %s is not equal to positionCount %s", ids.length, positionCount));
        }

        if (ids.length != isNull.length) {
            throw new IllegalArgumentException("ids length does not match isNull length");
        }

        setValues(values);
        this.ids = ids;
        this.isNull = isNull;
        this.dictionary = true;
    }

    public Block createNonLazyBlock()
    {
        assureLoaded();
        if (!dictionary) {
            return new SliceArrayBlock(getPositionCount(), values);
        }
        return createDictionaryBlock();
    }
}
