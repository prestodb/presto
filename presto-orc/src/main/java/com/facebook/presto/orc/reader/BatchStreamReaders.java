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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;

public final class BatchStreamReaders
{
    private BatchStreamReaders()
    {
    }

    public static BatchStreamReader createStreamReader(Type type, StreamDescriptor streamDescriptor, OrcRecordReaderOptions options, OrcAggregatedMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        switch (streamDescriptor.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanBatchStreamReader(type, streamDescriptor, systemMemoryContext.newOrcLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case BYTE:
                return new ByteBatchStreamReader(type, streamDescriptor, systemMemoryContext.newOrcLocalMemoryContext(BatchStreamReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongBatchStreamReader(type, streamDescriptor, systemMemoryContext);
            case FLOAT:
                return new FloatBatchStreamReader(type, streamDescriptor);
            case DOUBLE:
                return new DoubleBatchStreamReader(type, streamDescriptor);
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceBatchStreamReader(type, streamDescriptor, systemMemoryContext);
            case TIMESTAMP:
            case TIMESTAMP_MICROSECONDS:
                boolean enableMicroPrecision = type == TIMESTAMP_MICROSECONDS;
                return new TimestampBatchStreamReader(type, streamDescriptor, enableMicroPrecision);
            case LIST:
                return new ListBatchStreamReader(type, streamDescriptor, options, systemMemoryContext);
            case STRUCT:
                return new StructBatchStreamReader(type, streamDescriptor, options, systemMemoryContext);
            case MAP:
                return new MapBatchStreamReader(type, streamDescriptor, options, systemMemoryContext);
            case DECIMAL:
                return new DecimalBatchStreamReader(type, streamDescriptor);
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getOrcTypeKind());
        }
    }
}
