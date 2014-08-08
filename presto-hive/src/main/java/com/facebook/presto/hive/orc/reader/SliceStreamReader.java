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

import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT_V2;

public class SliceStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final SliceDirectStreamReader directReader;
    private final SliceDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public SliceStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        directReader = new SliceDirectStreamReader(streamDescriptor);
        dictionaryReader = new SliceDictionaryStreamReader(streamDescriptor);
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        currentReader.readBatch(vector);
    }

    @Override
    public void setNextBatchSize(int batchSize)
    {
        currentReader.setNextBatchSize(batchSize);
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncoding.Kind kind = encoding.get(streamDescriptor.getStreamId()).getKind();
        if (kind == DIRECT || kind == DIRECT_V2) {
            currentReader = directReader;
        }
        else if (kind == DICTIONARY || kind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
