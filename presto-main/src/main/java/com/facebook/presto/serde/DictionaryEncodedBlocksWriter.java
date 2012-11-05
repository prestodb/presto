/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.dictionary.Dictionary.DictionaryBuilder;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncodedBlocksWriter implements BlocksWriter
{
    private final BlocksWriter idWriter;
    private final SliceOutput sliceOutput;
    private TupleInfo tupleInfo;
    private DictionaryBuilder dictionaryBuilder;
    private boolean finished;

    public DictionaryEncodedBlocksWriter(BlocksWriter idWriter, SliceOutput sliceOutput)
    {
        this.idWriter = checkNotNull(idWriter, "idWriter is null");
        this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
    }

    @Override
    public BlocksWriter append(Iterable<Tuple> tuples)
    {
        checkNotNull(tuples, "tuples is null");
        checkState(!finished, "already finished");

        Iterable<Tuple> idTuples = Iterables.transform(tuples, new Function<Tuple, Tuple>()
        {
            @Override
            public Tuple apply(Tuple tuple)
            {
                if (tupleInfo == null) {
                    tupleInfo = tuple.getTupleInfo();
                    dictionaryBuilder = new DictionaryBuilder(tupleInfo);
                }

                long id = dictionaryBuilder.getId(tuple);
                return createTuple(id);
            }
        });
        idWriter.append(idTuples);

        return this;
    }

    @Override
    public void finish()
    {
        checkState(tupleInfo != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        idWriter.finish();

        // Serialize dictionary
        int dictionaryBytes = DictionarySerde.writeDictionary(sliceOutput, dictionaryBuilder.build());

        // Write length of dictionary
        sliceOutput.writeInt(dictionaryBytes);
    }
}
