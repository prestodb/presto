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
package com.facebook.presto.iceberg.vectors;

import com.facebook.airlift.log.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.List;

/**
 * Maintains the mapping between jvector node IDs and database row IDs.
 */
public class NodeRowIdMapping
{
    private static final Logger log = Logger.get(NodeRowIdMapping.class);
    private static final int VERSION = 1;
    private static final int BUFFER_SIZE = 65536; // 64KB buffer for I/O

    private final long[] nodeToRowId;

    /**
     * Creates a new mapping from a list of row IDs.
     * The list order must match the order vectors were added to the index.
     * @param rowIds List of row IDs in the same order as vectors were added to the index
     */
    public NodeRowIdMapping(List<Long> rowIds)
    {
        if (rowIds == null || rowIds.isEmpty()) {
            throw new IllegalArgumentException("Row IDs list cannot be null or empty");
        }

        this.nodeToRowId = new long[rowIds.size()];
        for (int i = 0; i < rowIds.size(); i++) {
            this.nodeToRowId[i] = rowIds.get(i);
        }

        log.info("Created NodeRowIdMapping with %d entries", nodeToRowId.length);
    }

    private NodeRowIdMapping(long[] nodeToRowId)
    {
        this.nodeToRowId = nodeToRowId;
    }

    /**
     * Gets the row ID for a given node ID.
     * @param nodeId The node ID from the vector index (0-based)
     * @return The corresponding row ID from the database table
     */
    public long getRowId(int nodeId)
    {
        if (nodeId < 0 || nodeId >= nodeToRowId.length) {
            throw new IndexOutOfBoundsException(
                    String.format("Invalid node ID %d. Valid range: [0, %d)",
                            nodeId, nodeToRowId.length));
        }
        return nodeToRowId[nodeId];
    }

    /**
     * Returns the number of mappings.
     */
    public int size()
    {
        return nodeToRowId.length;
    }

    /**
     * Saves the mapping to an output stream in binary format.
     * @param out The output stream to write to
     */
    public void save(OutputStream out) throws IOException
    {
        BufferedOutputStream bufferedOut = new BufferedOutputStream(out, BUFFER_SIZE);
        DataOutputStream dos = new DataOutputStream(bufferedOut);

        try {
            // Write header (version and size)
            dos.writeInt(VERSION);
            dos.writeInt(nodeToRowId.length);

            // Write all row IDs in using ByteBuffer
            int dataSize = nodeToRowId.length * 8;
            ByteBuffer buffer = ByteBuffer.allocate(Math.min(dataSize, BUFFER_SIZE));
            buffer.order(ByteOrder.BIG_ENDIAN);

            int offset = 0;
            while (offset < nodeToRowId.length) {
                buffer.clear();
                LongBuffer longBuffer = buffer.asLongBuffer();

                int remaining = nodeToRowId.length - offset;
                int count = Math.min(remaining, BUFFER_SIZE / 8);
                longBuffer.put(nodeToRowId, offset, count);

                // Write buffer to stream
                int bytesToWrite = count * 8;
                dos.write(buffer.array(), 0, bytesToWrite);

                offset += count;
            }

            dos.flush();
        }
        finally {
            dos.flush();
        }
    }

    /**
    Loads mapping using bulk I/O operations.
     */
    public static NodeRowIdMapping load(InputStream in) throws IOException
    {
        BufferedInputStream bufferedIn = new BufferedInputStream(in, BUFFER_SIZE);
        DataInputStream dis = new DataInputStream(bufferedIn);

        int version = dis.readInt();
        if (version != VERSION) {
            throw new IOException(
                    String.format("Unsupported mapping file version: %d (expected %d)",
                            version, VERSION));
        }

        int size = dis.readInt();
        if (size <= 0) {
            throw new IOException("Invalid mapping size: " + size);
        }

        //Read all row IDs in using ByteBuffer
        long[] nodeToRowId = new long[size];
        int dataSize = size * 8;

        byte[] buffer = new byte[Math.min(dataSize, BUFFER_SIZE)];

        int offset = 0;
        while (offset < size) {
            int remaining = size - offset;
            int count = Math.min(remaining, BUFFER_SIZE / 8);
            int bytesToRead = count * 8;

            dis.readFully(buffer, 0, bytesToRead);

            // Convert bytes to longs
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesToRead);
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
            LongBuffer longBuffer = byteBuffer.asLongBuffer();
            longBuffer.get(nodeToRowId, offset, count);

            offset += count;
        }

        return new NodeRowIdMapping(nodeToRowId);
    }

    @Override
    public String toString()
    {
        return String.format("NodeRowIdMapping[size=%d]", nodeToRowId.length);
    }
}
