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
import com.facebook.presto.orc.StreamDescriptor;
import org.joda.time.DateTimeZone;

public final class BatchStreamReaders
{
    private BatchStreamReaders()
    {
    }

    public static BatchStreamReader createStreamReader(
            StreamDescriptor streamDescriptor,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanBatchStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case BYTE:
                return new ByteBatchStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongBatchStreamReader(streamDescriptor, systemMemoryContext);
            case FLOAT:
                return new FloatBatchStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case DOUBLE:
                return new DoubleBatchStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceBatchStreamReader(streamDescriptor, systemMemoryContext);
            case TIMESTAMP:
                return new TimestampBatchStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case LIST:
                return new ListBatchStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case STRUCT:
                return new StructBatchStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case MAP:
                return new MapBatchStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case DECIMAL:
                return new DecimalBatchStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
