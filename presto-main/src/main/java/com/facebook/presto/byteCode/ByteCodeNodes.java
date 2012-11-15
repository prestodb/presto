package com.facebook.presto.byteCode;


public class ByteCodeNodes
{
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
