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
package com.facebook.presto.operator.repartition;

import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A tree structure to represent a decoded block. Dictionary/Rle blocks will be kept as a
 * separate node because we need the position mappings. For example, a block of map<int, bigint> where the key
 * is dictionary encoded will result in the following tree structure:
 *
 *             ColumnarMap
 *               /      \
 *    DictionaryBlock   LongArrayBlock
 *          |
 *     IntArrayBlock
 */
class DecodedBlockNode
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecodedBlockNode.class).instanceSize();

    // The decodedBlock could be primitive block, Dictionary/Rle block, or ColumnarArray/Map/Row object
    private final Object decodedBlock;
    private final List<DecodedBlockNode> children;
    private final long retainedSizeInBytes;
    private final long estimatedSerializedSizeInBytes;

    public DecodedBlockNode(Object decodedBlock, List<DecodedBlockNode> children, long decodedBlockRetainedSizeInBytes, long estimatedSerializedSizeInBytes)
    {
        this.decodedBlock = requireNonNull(decodedBlock, "decodedBlock is null");
        this.children = requireNonNull(children, "children is null");
        this.retainedSizeInBytes = decodedBlockRetainedSizeInBytes + INSTANCE_SIZE;
        this.estimatedSerializedSizeInBytes = estimatedSerializedSizeInBytes;
    }

    public Object getDecodedBlock()
    {
        return decodedBlock;
    }

    public List<DecodedBlockNode> getChildren()
    {
        return children;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    public long getEstimatedSerializedSizeInBytes()
    {
        return estimatedSerializedSizeInBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DecodedBlockNode{");
        sb.append("decodedBlock=").append(decodedBlock.toString()).append(",");
        sb.append("childrenCount=").append(children.size()).append(",");
        for (int i = 0; i < children.size(); i++) {
            sb.append("fieldBuffer_").append(i).append("=").append(children.get(i).toString()).append(",");
        }
        sb.append("}");

        return sb.toString();
    }
}
