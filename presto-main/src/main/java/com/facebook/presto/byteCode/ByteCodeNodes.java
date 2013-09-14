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
package com.facebook.presto.byteCode;

public class ByteCodeNodes
{
    public static Block buildBlock(CompilerContext context, ByteCodeNode node)
    {
        return buildBlock(context, node, null);
    }

    public static Block buildBlock(CompilerContext context, ByteCodeNode node, String description)
    {
        Block block;
        if (node instanceof Block) {
            block = (Block) node;
        }
        else {
            block = new Block(context).append(node);
        }
        block.setDescription(description);
        return block;
    }

    public static Block buildBlock(CompilerContext context, ByteCodeNodeFactory factory, ExpectedType expectedType)
    {
        return buildBlock(context, factory, expectedType, null);
    }

    public static Block buildBlock(CompilerContext context, ByteCodeNodeFactory factory, ExpectedType expectedType, String description)
    {
        ByteCodeNode node = factory.build(context, expectedType);
        Block block;
        if (node instanceof Block) {
            block = (Block) node;
        }
        else {
            block = new Block(context).append(node);
        }
        block.setDescription(description);
        return block;
    }
}
