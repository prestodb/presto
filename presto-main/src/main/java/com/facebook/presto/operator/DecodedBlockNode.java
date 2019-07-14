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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

class DecodedBlockNode
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecodedBlockNode.class).instanceSize();

    private final Object decodedBlock;
    private final List<DecodedBlockNode> children;

    public DecodedBlockNode(Object decodedBlock, List<DecodedBlockNode> children)
    {
        this.decodedBlock = requireNonNull(decodedBlock, "decodedBlock is null");
        this.children = requireNonNull(children, "children is null");
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
        int size = INSTANCE_SIZE;
        if (decodedBlock instanceof Block) {
            size += ((Block) decodedBlock).getRetainedSizeInBytes();
        }

        // TODO: count for Columnar objects

        for (DecodedBlockNode child : children) {
            size += child.getRetainedSizeInBytes();
        }
        return size;
    }
}
