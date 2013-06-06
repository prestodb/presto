/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.serde.TupleInfoSerde.readTupleInfo;
import static com.facebook.presto.serde.TupleInfoSerde.writeTupleInfo;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static org.testng.Assert.assertEquals;

public class TestTupleInfoSerde
{
    @Test
    public void testRoundTrip()
    {
        TupleInfo expectedTupleInfo = new TupleInfo(BOOLEAN, FIXED_INT_64, VARIABLE_BINARY, DOUBLE);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeTupleInfo(sliceOutput, expectedTupleInfo);
        TupleInfo actualTupleInfo = readTupleInfo(sliceOutput.slice().getInput());
        assertEquals(actualTupleInfo, expectedTupleInfo);
    }
}
