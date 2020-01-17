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
package org.apache.kudu.client;

import org.apache.kudu.Schema;

/**
 * Little wrapper to access KeyEncoder in Kudu Java client.
 */
public class KeyEncoderAccessor
{
    private KeyEncoderAccessor()
    {
    }

    public static byte[] encodePrimaryKey(PartialRow row)
    {
        return KeyEncoder.encodePrimaryKey(row);
    }

    public static PartialRow decodePrimaryKey(Schema schema, byte[] key)
    {
        return KeyEncoder.decodePrimaryKey(schema, key);
    }

    public static byte[] encodeRangePartitionKey(PartialRow row, PartitionSchema.RangeSchema rangeSchema)
    {
        return KeyEncoder.encodeRangePartitionKey(row, rangeSchema);
    }

    public static PartialRow decodeRangePartitionKey(Schema schema, PartitionSchema partitionSchema, byte[] key)
    {
        return KeyEncoder.decodeRangePartitionKey(schema, partitionSchema, key);
    }
}
