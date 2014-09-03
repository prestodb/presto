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
package com.facebook.presto.hive.orc.reader;

import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.IN_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.google.common.base.Preconditions.checkNotNull;

public class LongDictionaryStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;

    private LongStreamSource dictionaryDataStreamSource;
    private int dictionarySize;
    private long[] dictionary = new long[0];

    private BooleanStreamSource inDictionaryStreamSource;
    private BooleanStream inDictionaryStream;
    private final boolean[] inDictionary = new boolean[Vector.MAX_VECTOR_LENGTH];

    private LongStreamSource dataStreamSource;
    private LongStream dataStream;

    public LongDictionaryStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void setNextBatchSize(int batchSize)
    {
        skipSize += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (dataStream == null) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStreamSource != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                skipSize = presentStream.countBitsSet(skipSize);
            }

            if (inDictionaryStreamSource != null) {
                inDictionaryStream.skip(skipSize);
            }

            dataStream.skip(skipSize);
        }

        LongVector longVector = (LongVector) vector;

        if (presentStream == null) {
            Arrays.fill(longVector.isNull, false);
            dataStream.nextLongVector(nextBatchSize, longVector.vector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, longVector.isNull);
            dataStream.nextLongVector(nextBatchSize, longVector.vector, longVector.isNull);
        }

        if (inDictionaryStream == null) {
            Arrays.fill(inDictionary, true);
        }
        else {
            inDictionaryStream.getSetBits(nextBatchSize, inDictionary, longVector.isNull);
        }

        for (int i = 0; i < nextBatchSize; i++) {
            if (!longVector.isNull[i]) {
                if (inDictionary[i]) {
                    longVector.vector[i] = dictionary[((int) longVector.vector[i])];
                }
            }
        }

        skipSize = 0;
        nextBatchSize = 0;
    }

    private void openStreams()
            throws IOException
    {
        // read the dictionary
        if (dictionaryDataStreamSource != null) {
            if (dictionary.length < dictionarySize) {
                dictionary = new long[dictionarySize];
            }

            // read dictionary data
            LongStream dictionaryStream = dictionaryDataStreamSource.openStream();
            dictionaryStream.nextLongVector(dictionarySize, dictionary);
        }

        // open present stream
        if (presentStreamSource != null) {
            if (presentStream == null) {
                presentStream = presentStreamSource.openStream();
            }
        }

        // open in dictionary stream
        if (inDictionaryStreamSource != null) {
            if (inDictionaryStream == null) {
                inDictionaryStream = inDictionaryStreamSource.openStream();
            }
        }

        // open data stream
        dataStream = dataStreamSource.openStream();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, LongStreamSource.class);
        inDictionaryStreamSource = null;
        presentStreamSource = null;
        dataStreamSource = null;

        dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        dictionary = new long[0];
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        inDictionaryStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, IN_DICTIONARY, BooleanStreamSource.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStreamSource.class);

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
