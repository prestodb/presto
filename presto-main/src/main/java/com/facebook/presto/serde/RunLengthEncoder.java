/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.tuple.Tuple;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncoder
        implements Encoder
{
    private final SliceOutput sliceOutput;
    private boolean finished;

    private int tupleCount = -1;
    private Tuple lastTuple;
    private RunLengthBlockEncoding encoding;

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
            if (encoding == null) {
                encoding = new RunLengthBlockEncoding(tuple.getTupleInfo());
                tupleCount = 1;
                lastTuple = tuple;
            }
            else {
                if (!tuple.equals(lastTuple)) {
                    writeBlock();
                    lastTuple = tuple;
                }
                tupleCount++;
            }
        }

        return this;
    }

    private void writeBlock()
    {
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(lastTuple, tupleCount);

        encoding.writeBlock(sliceOutput, block);
        tupleCount = 0;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(encoding != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        // Flush out final block if there exists one (null if they were all empty blocks)
        writeBlock();

        return encoding;
    }
}
