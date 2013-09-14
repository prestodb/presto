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
package com.facebook.presto.serde;

import com.facebook.presto.block.dictionary.Dictionary.DictionaryBuilder;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncoder
        implements Encoder
{
    private final Encoder idWriter;
    private TupleInfo tupleInfo;
    private DictionaryBuilder dictionaryBuilder;
    private boolean finished;

    public DictionaryEncoder(Encoder idWriter)
    {
        this.idWriter = checkNotNull(idWriter, "idWriter is null");
    }

    @Override
    public Encoder append(Iterable<Tuple> tuples)
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
    public BlockEncoding finish()
    {
        checkState(tupleInfo != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        return new DictionaryBlockEncoding(dictionaryBuilder.build(), idWriter.finish());
    }
}
