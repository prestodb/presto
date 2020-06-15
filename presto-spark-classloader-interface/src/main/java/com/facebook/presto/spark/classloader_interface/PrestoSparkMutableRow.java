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
package com.facebook.presto.spark.classloader_interface;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class PrestoSparkMutableRow
        implements Serializable
{
    private static final int INSTANCE_SIZE = Long.BYTES * 2 /* headers */
            + Integer.BYTES /* partition */
            + Integer.BYTES /* length */
            + Long.BYTES /* bytes pointer */
            + Long.BYTES * 2 /* bytes headers */
            + Integer.BYTES /* bytes length */;

    private final int partition;
    private final int length;
    private final byte[] bytes;

    public PrestoSparkMutableRow(int partition, int length, byte[] bytes)
    {
        this.partition = partition;
        this.length = length;
        this.bytes = requireNonNull(bytes, "bytes is null");
    }

    public int getPartition()
    {
        return partition;
    }

    public int getLength()
    {
        return length;
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    public long getRetainedSize()
    {
        return INSTANCE_SIZE + bytes.length;
    }
}
