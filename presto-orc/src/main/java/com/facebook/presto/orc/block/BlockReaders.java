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
package com.facebook.presto.orc.block;

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

public final class BlockReaders
{
    private BlockReaders()
    {
    }

    public static BlockReader createBlockReader(
            StreamDescriptor streamDescriptor,
            boolean checkForNulls,
            DateTimeZone hiveStorageTimeZone,
            Type type)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanBlockReader(streamDescriptor);
            case BYTE:
                return new ByteBlockReader(streamDescriptor);
            case SHORT:
            case INT:
            case LONG:
                return new LongBlockReader(streamDescriptor);
            case FLOAT:
                return new FloatBlockReader(streamDescriptor);
            case DOUBLE:
                return new DoubleBlockReader(streamDescriptor);
            case BINARY:
            case STRING:
                return new SliceBlockReader(streamDescriptor);
            case TIMESTAMP:
                return new TimestampBlockReader(streamDescriptor, hiveStorageTimeZone);
            case DATE:
                return new DateBlockReader(streamDescriptor);
            case STRUCT:
                return new StructBlockReader(streamDescriptor, checkForNulls, hiveStorageTimeZone, type);
            case LIST:
                return new ListBlockReader(streamDescriptor, checkForNulls, hiveStorageTimeZone, type);
            case MAP:
                return new MapBlockReader(streamDescriptor, checkForNulls, hiveStorageTimeZone, type);
            case UNION:
            case DECIMAL:
                return new DecimalBlockReader(streamDescriptor, (DecimalType) type);
            case VARCHAR:
            case CHAR:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
