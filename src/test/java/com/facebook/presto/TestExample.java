package com.facebook.presto;

import com.google.common.collect.ImmutableList;

import java.util.Iterator;

import static java.util.Arrays.asList;

public class TestExample
{
    public static void main(String[] args)
    {
        DataScan3 scan = newScan();
        DataScan3 scan2 = newScan();

        Merge merge = new Merge(ImmutableList.of(scan, scan2), new TupleInfo(1, 1));

        while (merge.hasNext()) {
            ValueBlock block = merge.next();
            for (Object value : block) {
                System.out.println(value);
            }
            System.out.println();
        }
    }

    private static DataScan3 newScan()
    {
        Iterator<ValueBlock> values = ImmutableList.<ValueBlock>builder()
                .add(new UncompressedValueBlock(0, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'a', 'b', 'c', 'd', 'e', 'f'})))
                .add(new UncompressedValueBlock(20, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'h', 'i', 'j', 'k', 'l', 'm'})))
                .add(new UncompressedValueBlock(30, new TupleInfo(1), Slices.wrappedBuffer(new byte[]{'n', 'o', 'p', 'q', 'r', 's'})))
                .build()
                .iterator();

        Iterator<PositionBlock> positions = ImmutableList.<PositionBlock>builder()
                .add(new UncompressedPositionBlock(asList(2L, 4L, 6L, 8L)))
                .add(new UncompressedPositionBlock(asList(10L, 11L, 12L, 13L)))
                .add(new UncompressedPositionBlock(asList(18L, 19L, 20L, 21L, 22L)))
                .add(new UncompressedPositionBlock(asList(31L, 33L, 35L)))
                .add(new UncompressedPositionBlock(asList(40L, 41L, 42L)))
                .build()
                .iterator();

        return new DataScan3(values, positions);
    }
}
