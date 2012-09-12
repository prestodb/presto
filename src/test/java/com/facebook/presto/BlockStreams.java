package com.facebook.presto;

public class BlockStreams
{
    public static boolean equivalent(BlockStream<? extends ValueBlock> blockStream1, BlockStream<? extends ValueBlock> blockStream2) {
        if (!blockStream1.getTupleInfo().equals(blockStream2.getTupleInfo())) {
            return false;
        }

        Cursor cursor1 = blockStream1.cursor();
        Cursor cursor2 = blockStream2.cursor();

        while (cursor1.hasNextPosition()) {
            if (!cursor2.hasNextPosition()) {
                return false;
            }
            cursor1.advanceNextPosition();
            cursor2.advanceNextPosition();
            if (!cursor1.getTuple().equals(cursor2.getTuple())) {
                return false;
            }
            if (cursor1.getPosition() != cursor2.getPosition()) {
                return false;
            }
        }

        return !cursor2.hasNextPosition();

    }
}
