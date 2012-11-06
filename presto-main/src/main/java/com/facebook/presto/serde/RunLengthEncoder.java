/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.Tuple;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncoder
        implements Encoder
{
    private final SliceOutput sliceOutput;
    private boolean finished;

    private long startPosition = -1;
    private int tupleCount = -1;
    private Tuple lastTuple;

    public RunLengthEncoder(SliceOutput sliceOutput)
    {
        this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
    }

    @Override
    public Encoder append(Iterable<Tuple> tuples)
    {
        checkNotNull(tuples, "tuples is null");
        checkState(!finished, "already finished");

        for (Tuple tuple : tuples) {
            if (lastTuple == null) {
                startPosition = 0;
                tupleCount = 1;
                lastTuple = tuple;
            }
            else {
                if (!tuple.equals(lastTuple)) {
                    writeRunLengthEncodedBlock(sliceOutput,
                            startPosition,
                            tupleCount,
                            lastTuple);

                    lastTuple = tuple;
                    startPosition += tupleCount;
                    tupleCount = 0;
                }
                tupleCount++;
            }
        }

        return this;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(lastTuple != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        // Flush out final block if there exists one (null if they were all empty blocks)
        writeRunLengthEncodedBlock(sliceOutput,
                startPosition,
                tupleCount,
                lastTuple);

        return new RunLengthBlockEncoding(lastTuple.getTupleInfo());
    }

    private static void writeRunLengthEncodedBlock(SliceOutput destination, long startPosition, int tupleCount, Tuple value)
    {
        Slice tupleSlice = value.getTupleSlice();
        destination
                .appendInt(tupleSlice.length())
                .appendInt(tupleCount)
                .appendLong(startPosition)
                .writeBytes(tupleSlice);
    }
}
