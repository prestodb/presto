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
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StructStreamReader
    extends ColumnReader
    implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final Map<String, StreamReader> structFields;
    private Set<String> referencedFields;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    ColumnGroupReader reader;
    // Channel number in output of getBlock for fields. -1 if not returned.
    int[] fieldChannels;
    Type[] fieldTypes;
    int[] fieldBlockOffset;
    boolean[] valueIsNull;
    // Number of rows in field blocks. This is < numValues if there
    // are null structs in the result.
    int fieldBlockSize;
    int[] fieldSurviving;
    QualifyingSet fieldQualifyingSet;
    // Passing rows of field filters are returned here, null if no field filters.
    QualifyingSet fieldOutputQualifyingSet;
    // Copy of inputQualifyingSet. Needed when continuing after
    // truncation since the original input may have been changed.
    QualifyingSet inputCopy;
    // Position in row group of first unprocessed field row.
    int posInFields;
    StreamReader[] streamReaders;
    // For each position in fieldQualifyingSet, the corresponding
    // position in inputQualifyingSet.
    int[] innerToOuter;
    int[] orgFieldRows;
    int numFieldRows;

    StructStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.structFields = streamDescriptor.getNestedStreams().stream()
                .collect(toImmutableMap(stream -> stream.getFieldName().toLowerCase(Locale.ENGLISH), stream -> createStreamReader(stream, hiveStorageTimeZone, systemMemoryContext)));
    }

    @Override
    public void setReferencedSubfields(List<SubfieldPath> subfields, int depth)
    {
        HashMap<String, ArrayList<SubfieldPath>> subfieldPaths = new HashMap();
        referencedFields = new HashSet();
        for (SubfieldPath subfield : subfields) {
            List<PathElement> pathElements = subfield.getPath();
            PathElement immediateSubfield = pathElements.get(depth + 1);
            String fieldName = immediateSubfield.getField();
            referencedFields.add(fieldName);
            StreamReader fieldReader = structFields.get(fieldName);
            if (fieldReader instanceof StructStreamReader || fieldReader instanceof MapStreamReader || fieldReader instanceof ListStreamReader) {
                if (pathElements.size() > depth + 1) {
                    ArrayList<SubfieldPath> pathsForSubfield = subfieldPaths.get(fieldName);
                    if (pathsForSubfield == null) {
                        pathsForSubfield = new ArrayList();
                        subfieldPaths.put(fieldName, pathsForSubfield);
                    }
                    pathsForSubfield.add(subfield);
                }
            }
        }
        for (Map.Entry<String, ArrayList<SubfieldPath>> entry : subfieldPaths.entrySet()) {
            StreamReader fieldReader =  structFields.get(entry.getKey());
            fieldReader.setReferencedSubfields(entry.getValue(), depth + 1);
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
        posInFields = 0;
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

    private void setupForScan ()
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
                Filters.StructFilter structFilter = (Filters.StructFilter) filter;
                Filter fieldFilter = structFilter.getMember(new SubfieldPath.PathElement(fieldName.get(), 0));
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
        if (outputChannel == -1) {
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
                                       null,
                                       true,
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
        if (reader == null || outputChannel == -1) {
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
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannel != -1) {
            if (fieldSurviving == null || fieldSurviving.length < numSurviving) {
                fieldSurviving = new int[numSurviving];
            }
            int fieldBase = fieldBlockOffset[base];
            int numFieldSurviving = 0;
            for (int i = 0; i < numSurviving; i++) {
                if (valueIsNull != null && valueIsNull[base + surviving[i]]) {
                    valueIsNull[base + i] = true;
                    fieldBlockOffset[i] = fieldBase;
                }
                else {
                    fieldSurviving[numFieldSurviving++] = fieldBlockOffset[base + surviving[i]];
                    fieldBase++;
                    if (valueIsNull != null) {
                        valueIsNull[base + i] = false;
                    }
                    fieldBlockOffset[i] = fieldBase;
                }
            }
            for (StreamReader reader : streamReaders) {
                if (reader != null) {
                    reader.compactValues(fieldSurviving, base, numFieldSurviving);
                }
            }
            numValues = base + numSurviving;
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

    private int innerDistance(int from, int to)
    {
        if (presentStream == null) {
            return to - from;
        }
        int distance = 0;
        for (int i = from; i < to; i++) {
            if (present[i - posInRowGroup]) {
                distance++;
            }
        }
        return distance;
    }

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
        beginScan(presentStream, null);
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        if (fieldBlockOffset == null) {
            inputCopy = new QualifyingSet();
            fieldQualifyingSet = new QualifyingSet();
            if (filter != null) {
                fieldOutputQualifyingSet = new QualifyingSet();
            }
        }
        int initialFieldResults = reader.getNumResults();
        if (reader.hasUnfetchedRows()) {
            fieldQualifyingSet.clearTruncationPosition();
            reader.advance();
            int newTruncation = reader.getTruncationRow();
            if (newTruncation != -1) {
                truncationRow = innerToOuterRow(newTruncation);
                inputQualifyingSet.setTruncationRow(truncationRow);
            }
            else {
                posInFields = fieldQualifyingSet.getEnd();
            }
        }
        else {
            inputCopy.copyFrom(inputQualifyingSet);
            int numInput = input.getPositionCount();
            int[] inputRows = input.getPositions();
            int end = input.getEnd();
            int rowsInRange = end - posInRowGroup;
            int[] fieldRows = fieldQualifyingSet.getMutablePositions(numInput);
            int[] fieldInputNumbers = fieldQualifyingSet.getMutableInputNumbers(numInput);
            int prevFieldRow = posInFields;
            int prevRow = posInRowGroup;
            numFieldRows = 0;
            if (innerToOuter == null || innerToOuter.length < numInput) {
                innerToOuter = new int [numInput + 100];
            }
            for (int i = 0; i < numInput; i++) {
                int activeRow = inputRows[i];
                if (presentStream == null || present[activeRow - posInRowGroup]) {
                    int numSkip = innerDistance(prevRow, activeRow);
                    fieldRows[numFieldRows] = prevFieldRow + numSkip;
                    fieldInputNumbers[numFieldRows] = i;
                    innerToOuter[numFieldRows] = i;
                    numFieldRows++;
                    prevFieldRow += numSkip;
                    prevRow = activeRow;
                }
            }
            int skip = innerDistance(prevRow, end);
            fieldQualifyingSet.setEnd(skip + prevFieldRow);
            fieldQualifyingSet.setPositionCount(numFieldRows);
            fieldQualifyingSet.clearTruncationPosition();
            if (orgFieldRows == null || orgFieldRows.length < numFieldRows) {
                orgFieldRows = new int[numFieldRows];
            }
            System.arraycopy(fieldRows, 0, orgFieldRows, 0, numFieldRows);
            reader.setQualifyingSets(fieldQualifyingSet, fieldOutputQualifyingSet);
            if (fieldQualifyingSet.getPositionCount() > 0) {
                reader.advance();
                int truncated = reader.getTruncationRow();
                if (truncated != -1) {
                    posInFields = truncated;
                    truncationRow = innerToOuterRow(truncated);
                    inputQualifyingSet.setTruncationRow(truncationRow);
                }
                else {
                    truncationRow = -1;
                    posInFields = fieldQualifyingSet.getEnd();
                }
            }
            }
        int[] resultRows = null;
        int[] inputNumbers = null;
        int[] inputRows = inputCopy.getPositions();
        int numInput = inputCopy.getPositionCount();
        if (output != null) {
            resultRows = output.getMutablePositions(numInput);
            inputNumbers = output.getMutableInputNumbers(numInput);
        }
        int[] fieldQualifyingRows = null;
        if (fieldOutputQualifyingSet != null) {
            fieldQualifyingRows = fieldOutputQualifyingSet.getPositions();
        }
        ensureOutput(numInput);
        // Ranges over positions in the fieldQualifyingSet.
        int fieldInIdx = 0;
        // Ranges over positions in fieldOutputQualifyingSet.
        int fieldOutIdx = 0;
        int numFieldResults = reader.getNumResults() - initialFieldResults;
        // We loop over the input rows: Either 1. the struct was null
        // and we emit a null or do nothing. 2. There was a struct and
        // it is in the field reader qualifying set (or there is no
        // filter). It goes to the result. 3. There was a struct but
        // it was dropped by a filter. We move on.
        for (int i = 0; i < numInput; i++) {
            int presentIdx = inputRows[i] - posInRowGroup;
            if (presentStream != null && !present[presentIdx]) {
                if (filter == null || filter.testNull()) {
                    valueIsNull[numValues + numResults] = true;
                    fieldBlockOffset[numValues + numResults] = numValues + numResults > 0 ? fieldBlockOffset[numValues + numResults - 1] : 0;
                    if (resultRows != null) {
                        resultRows[numResults] = inputRows[i];
                        inputNumbers[numResults] = i;
                    }
                    numResults++;
                }
            }
            else {
                // A non null struct in the input qualifying set.
                if (filter != null) {
                    if (fieldOutIdx >= numFieldResults) {
                        break;
                    }
                    if (fieldQualifyingRows[fieldOutIdx] == orgFieldRows[fieldInIdx]) {
                        fieldInIdx++;
                        fieldOutIdx++;
                        resultRows[numResults] = inputRows[i];
                        inputNumbers[numResults] = i;
                        addStructResult();
                    }
                    else {
                        fieldInIdx++;
                    }
                }
                else {
                    addStructResult();
                    if (--numFieldResults == 0) {
                        break;
                    }
                }
            }
        }
        endScan(presentStream);
    }

    void addStructResult()
    {
        fieldBlockOffset[numValues + numResults] = fieldBlockSize;
        fieldBlockOffset[numValues + numResults + 1] = fieldBlockSize + 1;
        if (valueIsNull != null) {
            valueIsNull[numValues + numResults] = false;
        }
        fieldBlockSize++;
        numResults++;
    }

    // Returns the enclosing row number for a field column row number.
    int innerToOuterRow(int inner)
    {
        for (int i = 0; i < numFieldRows; i++) {
            if (inner == orgFieldRows[i]) {
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
        } else if (fieldBlockOffset.length < numValues + numAdded + 1) {
            fieldBlockOffset = Arrays.copyOf(fieldBlockOffset, newSize);
        }
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        int innerFirstRows = 0;
        for (int i = 0; i < numFirstRows; i++) {
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
        return new RowBlock(0, numFirstRows, nulls, offsets, blocks);
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
}
