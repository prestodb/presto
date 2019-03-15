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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.ColumnGroupReader;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.Filters;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.PageSourceOptions.FilterFunction;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.SubfieldPath.NestedField;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.block.RowBlock.createRowBlockInternal;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StructStreamReader
        extends NullWrappingColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final Map<String, StreamReader> structFields;
    private Set<String> referencedFields;

    private int readOffset;
    private int nextBatchSize;

    ColumnGroupReader reader;
    // Channel number in output of getBlock for fields. -1 if not returned.
    int[] fieldChannels;
    Type[] fieldTypes;
    // Number of values in field readers. Differs from numValues if there are null structs.
    int fieldBlockSize;
    int[] fieldBlockOffset;
    int[] fieldSurviving;
    // Copy of inputQualifyingSet. Needed when continuing after
    // truncation since the original input may have been changed.
    QualifyingSet inputCopy;
    StreamReader[] streamReaders;

    StructStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        super(OptionalInt.empty());
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.structFields = streamDescriptor.getNestedStreams().stream()
                .collect(toImmutableMap(stream -> stream.getFieldName().toLowerCase(Locale.ENGLISH), stream -> createStreamReader(stream, hiveStorageTimeZone, systemMemoryContext)));
    }

    @Override
    public void setReferencedSubfields(List<SubfieldPath> subfields, int depth)
    {
        Map<String, List<SubfieldPath>> fieldToPaths = new HashMap();
        referencedFields = new HashSet();
        for (SubfieldPath subfield : subfields) {
            List<PathElement> pathElements = subfield.getPath();
            PathElement immediateSubfield = pathElements.get(depth + 1);
            checkArgument(immediateSubfield instanceof NestedField, "Unsupported subfield type: " + immediateSubfield.getClass().getSimpleName());
            String fieldName = ((NestedField) immediateSubfield).getName();
            referencedFields.add(fieldName);
            StreamReader fieldReader = structFields.get(fieldName);
            if (fieldReader instanceof StructStreamReader || fieldReader instanceof MapStreamReader || fieldReader instanceof ListStreamReader) {
                if (pathElements.size() > depth + 1) {
                    fieldToPaths.computeIfAbsent(fieldName, k -> new ArrayList<>())
                            .add(subfield);
                }
            }
        }
        for (Map.Entry<String, List<SubfieldPath>> entry : fieldToPaths.entrySet()) {
            structFields.get(entry.getKey()).setReferencedSubfields(entry.getValue(), depth + 1);
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the field readers
                readOffset = presentStream.countBitsSet(readOffset);
            }
            for (StreamReader structField : structFields.values()) {
                structField.prepareNextRead(readOffset);
            }
        }

        boolean[] nullVector = null;
        Block[] blocks;

        if (presentStream == null) {
            blocks = getBlocksForType(type, nextBatchSize);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                blocks = getBlocksForType(type, nextBatchSize - nullValues);
            }
            else {
                List<Type> typeParameters = type.getTypeParameters();
                blocks = new Block[typeParameters.size()];
                for (int i = 0; i < typeParameters.size(); i++) {
                    blocks[i] = typeParameters.get(i).createBlockBuilder(null, 0).build();
                }
            }
        }

        verify(Arrays.stream(blocks)
                .mapToInt(Block::getPositionCount)
                .distinct()
                .count() == 1);

        // Struct is represented as a row block
        Block rowBlock = RowBlock.fromFieldBlocks(nextBatchSize, Optional.ofNullable(nullVector), blocks);

        readOffset = 0;
        nextBatchSize = 0;

        return rowBlock;
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields.values()) {
            structField.startStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields.values()) {
            structField.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private Block[] getBlocksForType(Type type, int positionCount)
            throws IOException
    {
        RowType rowType = (RowType) type;

        Block[] blocks = new Block[rowType.getFields().size()];

        for (int i = 0; i < rowType.getFields().size(); i++) {
            Optional<String> fieldName = rowType.getFields().get(i).getName();
            Type fieldType = rowType.getFields().get(i).getType();

            if (!fieldName.isPresent()) {
                throw new IllegalArgumentException("Missing struct field name in type " + rowType);
            }

            String lowerCaseFieldName = fieldName.get().toLowerCase(Locale.ENGLISH);
            StreamReader streamReader = structFields.get(lowerCaseFieldName);
            boolean isReferenced = referencedFields == null || referencedFields.contains(lowerCaseFieldName);
            if (streamReader != null && isReferenced) {
                streamReader.prepareNextRead(positionCount);
                blocks[i] = streamReader.readBlock(fieldType);
            }
            else {
                blocks[i] = getNullBlock(fieldType, positionCount);
            }
        }
        return blocks;
    }

    private static Block getNullBlock(Type type, int positionCount)
    {
        Block nullValueBlock = type.createBlockBuilder(null, 1)
                .appendNull()
                .build();
        return new RunLengthEncodedBlock(nullValueBlock, positionCount);
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            for (StreamReader structField : structFields.values()) {
                closer.register(() -> structField.close());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (StreamReader structField : structFields.values()) {
            retainedSizeInBytes += structField.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    private void setupForScan()
    {
        RowType rowType = (RowType) type;
        int numFields = rowType.getFields().size();
        int[] fieldColumns = new int[numFields];
        int[] channelColumns = new int[numFields];
        fieldTypes = new Type[numFields];
        streamReaders = new StreamReader[numFields];
        HashMap<Integer, Filter> filters = new HashMap();
        for (int i = 0; i < numFields; i++) {
            fieldColumns[i] = i;
            channelColumns[i] = i;
            Optional<String> fieldName = rowType.getFields().get(i).getName();
            Type fieldType = rowType.getFields().get(i).getType();

            if (!fieldName.isPresent()) {
                throw new IllegalArgumentException("Missing struct field name in type " + rowType);
            }

            fieldTypes[i] = fieldType;
            if (filter != null) {
                Filter fieldFilter = ((Filters.StructFilter) filter).getMember(new NestedField(fieldName.get()));
                if (fieldFilter != null) {
                    filters.put(i, fieldFilter);
                }
            }
            String lowerCaseFieldName = fieldName.get().toLowerCase(Locale.ENGLISH);
            if (referencedFields != null && !referencedFields.contains(lowerCaseFieldName)) {
                fieldColumns[i] = -1;
            }
            if (fieldColumns[i] != -1 || filters.get(i) != null) {
                StreamReader streamReader = structFields.get(lowerCaseFieldName);
                streamReaders[i] = streamReader;
            }
        }
        fieldChannels = fieldColumns;
        if (!outputChannelSet) {
            // If the struct is not projected out, none of its members is either.
            Arrays.fill(fieldChannels, -1);
        }
        reader = new ColumnGroupReader(streamReaders,
                                       null,
                                       channelColumns,
                                       rowType.getTypeParameters(),
                                       fieldColumns,
                                       fieldColumns,
                                       filters,
                                       new FilterFunction[0],
                                       true,
                                       0);
    }

    @Override
    public void setResultSizeBudget(long bytes)
    {
        if (reader == null) {
            setupForScan();
        }
        reader.setResultSizeBudget(bytes);
    }

    @Override
    public void erase(int end)
    {
        // Without a reader there is nothing to erase, even if the struct is all nulls.
        if (reader == null || !outputChannelSet) {
            return;
        }
        int fieldEnd;
        if (valueIsNull != null) {
            fieldEnd = 0;
            for (int i = 0; i < end; i++) {
                if (!valueIsNull[i]) {
                    fieldEnd++;
                }
            }
        }
        else {
            fieldEnd = end;
        }
        // There is a fieldBlockOffset also for null structs.
        fieldBlockSize -= end;
        // There is a field block value only for non-null structs.
        reader.newBatch(fieldEnd);
        numValues -= end;
        if (valueIsNull != null) {
            System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
        }
        int fieldFill = 0;
        for (int i = 0; i < numValues; i++) {
            fieldBlockOffset[i] = fieldFill;
            if (valueIsNull == null || !valueIsNull[i]) {
                fieldFill++;
            }
        }
        fieldBlockOffset[numValues] = fieldFill;
        verify(fieldBlockSize == numValues);
        if (fieldFill > 0) {
            reader.getBlocks(fieldFill, true, false);
        }
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannelSet) {
            check();
            if (fieldSurviving == null || fieldSurviving.length < numSurviving) {
                fieldSurviving = new int[numSurviving];
            }
            int fieldBase = fieldBlockOffset[base];
            int initialFieldBase = fieldBase;
            int numFieldSurviving = 0;
            for (int i = 0; i < numSurviving; i++) {
                if (valueIsNull != null && valueIsNull[base + surviving[i]]) {
                    valueIsNull[base + i] = true;
                    fieldBlockOffset[base + i] = fieldBase;
                }
                else {
                    fieldSurviving[numFieldSurviving++] = fieldBlockOffset[base + surviving[i]] - initialFieldBase;
                    if (valueIsNull != null) {
                        valueIsNull[base + i] = false;
                    }
                    fieldBlockOffset[base + i] = fieldBase;
                    fieldBase++;
                }
            }
            fieldBlockOffset[base + numSurviving] = fieldBase;
            fieldBlockSize = base + numSurviving;
            reader.compactValues(fieldSurviving, initialFieldBase, numFieldSurviving);
            numValues = base + numSurviving;
            check();
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (reader == null) {
            return 0;
        }
        return reader.getResultSizeInBytes();
    }

    public int getAverageResultSize()
    {
        if (reader == null) {
            return 10 * structFields.size();
        }
        return reader.getAverageResultSize();
    }

    static int callCount;
    static int stopCallCount = -1;

    @Override
    public void scan()
            throws IOException
    {
        if (reader == null) {
            setupForScan();
        }
        if (!rowGroupOpen) {
            openRowGroup();
        }
        callCount++;
        if (stopCallCount != -1 && callCount >= stopCallCount) {
            System.out.println("break");
        }
        check();
        beginScan(presentStream, null);
        int initialFieldResults = reader.getNumResults();
        if (reader.hasUnfetchedRows()) {
            // posInRowGroup is the first unprocessed enclosing level
            // row, by definition part of the input qualifying set.
            setInnerTruncation();
            int originalTarget = innerQualifyingSet.getEnd();
            reader.advance();
            int newTruncation = reader.getTruncationRow();
            if (newTruncation != -1) {
                innerPosInRowGroup = newTruncation;
                truncationRow = innerToOuterRow(newTruncation);
                inputQualifyingSet.setTruncationRow(truncationRow);
            }
            else {
                innerPosInRowGroup = originalTarget;
            }
        }
        else {
            if (inputCopy == null) {
                inputCopy = new QualifyingSet();
            }
            inputCopy.copyFrom(inputQualifyingSet);
            makeInnerQualifyingSet();
            if (hasNulls) {
                innerQualifyingSet.setParent(inputQualifyingSet);
                innerQualifyingSet.setTranslateResultToParentRows(true);
            }
            else {
                // There are no nulls
                if (innerQualifyingSet == null) {
                    innerQualifyingSet = new QualifyingSet();
                }
                innerQualifyingSet.copyFrom(inputQualifyingSet);
                innerQualifyingSet.setTranslateResultToParentRows(false);
            }
            reader.setQualifyingSets(innerQualifyingSet, outputQualifyingSet);
            if (innerQualifyingSet.getPositionCount() > 0) {
                reader.advance();
                int truncated = reader.getTruncationRow();
                if (truncated != -1) {
                    innerPosInRowGroup = truncated;
                    truncationRow = innerToOuterRow(truncated);
                    inputQualifyingSet.setTruncationRow(truncationRow);
                }
                else {
                    truncationRow = -1;
                    innerPosInRowGroup = innerQualifyingSet.getEnd();
                }
            }
        }
        ensureOutput(numInnerRows + numNullsToAdd);
        // The outputQualifyingSet is written by advance.
        int numStructs = reader.getNumResults() - initialFieldResults;
        for (int i = 0; i < numStructs; i++) {
            addStructResult();
        }
        int lastFieldOffset = fieldBlockSize == 0 ? 0 : fieldBlockOffset[fieldBlockSize];
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, truncationRow != -1 ? truncationRow : inputQualifyingSet.getEnd());
        if (numResults > numInnerResults) {
            // Fill null positions in fieldBlockOffset  with the offset of the next non-null.
            fieldBlockOffset[numValues + numResults] = lastFieldOffset;
            int nextNonNull = lastFieldOffset;
            for (int i = numValues + numResults - 1; i >= numValues; i--) {
                if (fieldBlockOffset[i] == -1) {
                    fieldBlockOffset[i] = nextNonNull;
                }
                else {
                    nextNonNull = fieldBlockOffset[i];
                }
            }
        }
        fieldBlockSize = numValues + numResults;
        endScan(presentStream);
        check();
    }

    void addStructResult()
    {
        int lastFieldOffset = fieldBlockSize == 0 ? 0 : fieldBlockOffset[fieldBlockSize];
        fieldBlockOffset[numValues + numInnerResults] = lastFieldOffset;
        fieldBlockOffset[numValues + numInnerResults + 1] = lastFieldOffset + 1;
        if (valueIsNull != null) {
            valueIsNull[numValues + numInnerResults] = false;
        }
        fieldBlockSize++;
        numInnerResults++;
    }

    @Override
    protected void shiftUp(int from, int to)
    {
        fieldBlockOffset[to] = fieldBlockOffset[from];
    }

    @Override
    protected void writeNull(int position)
    {
        fieldBlockOffset[position] = -1;
    }

    // Returns the enclosing row number for a field column row number.
    int innerToOuterRow(int inner)
    {
        if (!hasNulls) {
            return inner;
        }
        int[] innerRows = innerQualifyingSet.getPositions();
        int numRows = innerQualifyingSet.getPositionCount();
        for (int i = 0; i < numRows; i++) {
            if (inner == innerRows[i]) {
                int[] innerToOuter = innerQualifyingSet.getInputNumbers();
                return inputCopy.getPositions()[innerToOuter[i]];
            }
        }
        throw new IllegalArgumentException("Can't translate from struct truncation row to enclosing truncation row");
    }

    void ensureOutput(int numAdded)
    {
        int newSize = numValues + numAdded * 2;
        if (presentStream != null && valueIsNull == null) {
            valueIsNull = new boolean[newSize];
        }
        if (valueIsNull != null && valueIsNull.length < numValues + numAdded) {
            valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        }
        if (fieldBlockOffset == null) {
            fieldBlockOffset = new int[newSize];
        }
        else if (fieldBlockOffset.length < numValues + numAdded + 1) {
            fieldBlockOffset = Arrays.copyOf(fieldBlockOffset, newSize);
        }
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        int innerFirstRows = 0;
        for (int i = 0; i < numFirstRows; i++) {
            if (fieldBlockOffset[i] != innerFirstRows) {
                throw new IllegalArgumentException("Struct nulls and block field indices inconsistent");
            }
            if (valueIsNull == null || !valueIsNull[i]) {
                innerFirstRows++;
            }
        }
        if (innerFirstRows == 0) {
            return getNullBlock(type, numFirstRows);
        }
        Block[] blocks = reader.getBlocks(innerFirstRows, mayReuse, true);
        blocks = fillUnreferencedWithNulls(blocks, innerFirstRows);
        int[] offsets = mayReuse ? fieldBlockOffset : Arrays.copyOf(fieldBlockOffset, numFirstRows + 1);
        boolean[] nulls = valueIsNull == null ? null
            : mayReuse ? valueIsNull : Arrays.copyOf(valueIsNull, numFirstRows);
        return createRowBlockInternal(0, numFirstRows, nulls, offsets, blocks);
    }

    private Block[] fillUnreferencedWithNulls(Block[] blocks, int numRows)
    {
        if (blocks.length < fieldChannels.length) {
            blocks = Arrays.copyOf(blocks, fieldChannels.length);
        }
        for (int i = 0; i < fieldChannels.length; i++) {
            if (fieldChannels[i] == -1) {
                blocks[i] = getNullBlock(fieldTypes[i], numRows);
            }
        }
        return blocks;
    }

    @Override
    public void maybeReorderFilters()
    {
        reader.maybeReorderFilters();
    }

    void check()
    {
        int innerFirstRows = 0;
        for (int i = 0; i < numValues; i++) {
            if (fieldBlockOffset[i] != innerFirstRows) {
                throw new IllegalArgumentException("Struct nulls and block field indices inconsistent");
            }
            if (valueIsNull == null || !valueIsNull[i]) {
                innerFirstRows++;
            }
        }
        if (numValues > 0 && (fieldBlockOffset[numValues] != innerFirstRows || fieldBlockSize != numValues)) {
            throw new IllegalArgumentException("Last fieldBlockOffset inconsistent");
        }
        reader.getBlocks(innerFirstRows, true, false);
    }

    void setcc(int cc, int stop)
    {
        stopCallCount = stop;
        callCount = cc;
    }
}
