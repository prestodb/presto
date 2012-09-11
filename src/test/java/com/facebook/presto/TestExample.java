package com.facebook.presto;

import com.facebook.presto.operators.DataScan3;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.TupleInfo.SINGLE_LONG_TUPLEINFO;
import static com.google.common.base.Charsets.UTF_8;

public class TestExample
{
    public static void main(String[] args)
    {
        DataScan3 scan = newScan();
//        DataScan3 scan2 = newScan();

//        Merge merge = new Merge(ImmutableList.of(scan, scan2), new TupleInfo(FIXED_INT_64, FIXED_INT_64));

        // 2 c
        //  4 e
        // 20 h
        // 21 i
        // 22 j
        // 31 o
        // 33 q
        // 35 s

        Cursor cursor = scan.cursor();
        while (cursor.advanceNextPosition()) {
            System.out.printf("%d %s\n", cursor.getPosition(), cursor.getSlice(0).toString(UTF_8));
        }
    }

    private static DataScan3 newScan()
    {
        List<UncompressedValueBlock> values = ImmutableList.<UncompressedValueBlock>builder()
                .add(Blocks.createBlock(0, "a", "b", "c", "d", "e", "f"))
                .add(Blocks.createBlock(20, "h", "i", "j", "k", "l", "m"))
                .add(Blocks.createBlock(30, "n", "o", "p", "q", "r", "s"))
                .build();

        List<UncompressedPositionBlock> positions = ImmutableList.<UncompressedPositionBlock>builder()
                .add(new UncompressedPositionBlock(2L, 4L, 6L, 8L))
                .add(new UncompressedPositionBlock(10L, 11L, 12L, 13L))
                .add(new UncompressedPositionBlock(18L, 19L, 20L, 21L, 22L))
                .add(new UncompressedPositionBlock(31L, 33L, 35L))
                .add(new UncompressedPositionBlock(40L, 41L, 42L))
                .build();

        return new DataScan3(new UncompressedBlockStream(SINGLE_LONG_TUPLEINFO, values), new ValueBlockStream<>(new TupleInfo(), positions));
    }
}
