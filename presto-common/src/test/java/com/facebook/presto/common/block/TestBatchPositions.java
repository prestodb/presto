package com.facebook.presto.common.block;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBatchPositions
{
    @DataProvider
    public static Object[][] addPositionDataProvider()
    {
        return new Object[][] {
                {ImmutableList.of(0, 1, 2)},
                {ImmutableList.of(1, 2, 3)},
                {ImmutableList.of(2, 1, 0)},
                {ImmutableList.of(3, 2, 1)},
                {ImmutableList.of(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3)},
                {ImmutableList.of(3, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1)},
                {ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)},
                {ImmutableList.of(10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)},
                {ImmutableList.of(0, 1, 2, 3, 4, 0, 1, 2, 3, 4)},
                {ImmutableList.of(10, 9, 8, 7, 6, 10, 9, 8, 7, 6)},
                {ImmutableList.of(0, 0, 0)},
                {ImmutableList.of(1, 1, 1)},
                {ImmutableList.of(0, 2, 2, 0)},
                {ImmutableList.of(0, 2, 2, 3)},
                {ImmutableList.of(1, 10, 20)},
                {ImmutableList.of(0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2)},
                {randomValues(3)},
                {randomValues(4)},
                {randomValues(7)},
                {randomValues(20)},
                {randomValues(100)}
        };
    }

    @Test(dataProvider = "addPositionDataProvider")
    public void testAddPosition(List<Integer> expected)
    {
        BatchPositions2 positions = new BatchPositions2();
        for (int i = 0; i < 2; i++) {
            for (Integer position : expected) {
                positions.addPosition(position);
            }

            List<Integer> actual = dumpPositions(positions);
            assertEquals(actual, expected);

            // do reset for the second run
            positions.reset();
        }
    }

    private static List<Integer> dumpPositions(BatchPositions2 positions)
    {
        List<Integer> values = new ArrayList<>();
        int size = positions.getSize();
        for (int i = 0; i < size; i++) {
            assertTrue(positions.getPositionLength(i) > 0);
            for (int j = positions.getPositionStart(i); j < positions.getPositionEnd(i); j++) {
                values.add(j);
            }
        }
        return values;
    }

    private static List<Integer> randomValues(int size)
    {
        List<Integer> values = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < size; i++) {
            values.add(Math.abs(rnd.nextInt()));
        }
        return values;
    }
}
