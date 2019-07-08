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
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public final class SelectiveStreamReaders
{
    private SelectiveStreamReaders() {}

    public static SelectiveStreamReader createStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            List<Subfield> requiredSubfields,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                checkArgument(requiredSubfields.isEmpty(), "Boolean stream reader doesn't support subfields");
                return new BooleanSelectiveStreamReader(streamDescriptor, filter, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveStreamReaders.class.getSimpleName()));
            case BYTE:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                checkArgument(requiredSubfields.isEmpty(), "Primitive type stream reader doesn't support subfields");
                return new LongSelectiveStreamReader(streamDescriptor, filter, outputType, systemMemoryContext);
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
            case LIST:
            case STRUCT:
            case MAP:
            case DECIMAL:
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
