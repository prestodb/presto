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

import com.facebook.presto.hive.orc.SliceVector;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.hive.orc.json.JsonReader;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import io.airlift.slice.DynamicSliceOutput;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.orc.json.JsonReaders.createJsonReader;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class JsonStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final JsonReader jsonReader;

    private boolean stripeOpen;
    private boolean rowGroupOpen;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;
    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    private int skipSize;
    private int nextBatchSize;

    private StreamSources dictionaryStreamSources;
    private StreamSources dataStreamSources;
    private List<ColumnEncoding> encoding;

    public JsonStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.jsonReader = createJsonReader(streamDescriptor, false, hiveStorageTimeZone, sessionTimeZone);
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
        if (!rowGroupOpen) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStreamSource != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                skipSize = presentStream.countBitsSet(skipSize);
            }

            jsonReader.skip(skipSize);
        }

        SliceVector sliceVector = (SliceVector) vector;
        if (presentStream != null) {
            presentStream.getUnsetBits(nextBatchSize, isNullVector);
        }

        DynamicSliceOutput out = new DynamicSliceOutput(1024);
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                out.reset();
                try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                    jsonReader.readNextValueInto(generator);
                }
                sliceVector.slice[i] = out.copySlice();
            }
            else {
                sliceVector.slice[i] = null;
            }
        }

        skipSize = 0;
        nextBatchSize = 0;
    }

    private void openStreams()
            throws IOException
    {
        if (presentStreamSource != null) {
            presentStream = presentStreamSource.openStream();
        }

        if (!stripeOpen) {
            jsonReader.openStripe(dictionaryStreamSources, encoding);
        }

        jsonReader.openRowGroup(dataStreamSources);

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        this.dictionaryStreamSources = dictionaryStreamSources;
        this.dataStreamSources = null;
        this.encoding = encoding;

        presentStreamSource = null;

        stripeOpen = false;
        rowGroupOpen = false;

        skipSize = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        this.dataStreamSources = dataStreamSources;

        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);

        rowGroupOpen = false;

        skipSize = 0;
        nextBatchSize = 0;

        if (presentStreamSource == null) {
            Arrays.fill(isNullVector, false);
        }
        presentStream = null;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
