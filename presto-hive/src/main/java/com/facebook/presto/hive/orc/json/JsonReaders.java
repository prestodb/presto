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
package com.facebook.presto.hive.orc.json;

import com.facebook.presto.hive.orc.StreamDescriptor;
import org.joda.time.DateTimeZone;

public final class JsonReaders
{
    private JsonReaders()
    {
    }

    public static JsonMapKeyReader createJsonMapKeyReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanJsonReader(streamDescriptor);
            case BYTE:
                return new ByteJsonReader(streamDescriptor);
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongJsonReader(streamDescriptor);
            case FLOAT:
                return new FloatJsonReader(streamDescriptor);
            case DOUBLE:
                return new DoubleJsonReader(streamDescriptor);
            case BINARY:
                return new SliceJsonReader(streamDescriptor, true);
            case STRING:
                return new SliceJsonReader(streamDescriptor, false);
            case TIMESTAMP:
                return new TimestampJsonReader(streamDescriptor, hiveStorageTimeZone, sessionTimeZone);
            case STRUCT:
            case LIST:
            case MAP:
            case UNION:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
            default:
                throw new IllegalArgumentException("Unsupported map key type: " + streamDescriptor.getStreamType());
        }
    }

    public static JsonReader createJsonReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanJsonReader(streamDescriptor);
            case BYTE:
                return new ByteJsonReader(streamDescriptor);
            case SHORT:
            case INT:
            case LONG:
                return new LongJsonReader(streamDescriptor);
            case FLOAT:
                return new FloatJsonReader(streamDescriptor);
            case DOUBLE:
                return new DoubleJsonReader(streamDescriptor);
            case BINARY:
                return new SliceJsonReader(streamDescriptor, true);
            case STRING:
                return new SliceJsonReader(streamDescriptor, false);
            case TIMESTAMP:
                return new TimestampJsonReader(streamDescriptor, hiveStorageTimeZone, sessionTimeZone);
            case DATE:
                return new DateJsonReader(streamDescriptor);
            case STRUCT:
                return new StructJsonReader(streamDescriptor, checkForNulls, hiveStorageTimeZone, sessionTimeZone);
            case LIST:
                return new ListJsonReader(streamDescriptor, checkForNulls, hiveStorageTimeZone, sessionTimeZone);
            case MAP:
                return new MapJsonReader(streamDescriptor, checkForNulls, sessionTimeZone, sessionTimeZone);
            case UNION:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
