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
package com.facebook.presto.operator.simple;

import com.facebook.presto.operator.MultiPageChunkCursor;
import com.facebook.presto.operator.PagePositionEqualitor;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * ProcessorInput that adapts a Page input into a MultiPageChunkCursor input
 */
public class ClusteredProcessorInput
        implements ProcessorInput<Page>
{
    private final PagePositionEqualitor equalitor;
    private final ProcessorInput<MultiPageChunkCursor> clusteredInput;
    private Page lastPage;

    public ClusteredProcessorInput(PagePositionEqualitor equalitor, ProcessorInput<MultiPageChunkCursor> clusteredInput)
    {
        this.equalitor = requireNonNull(equalitor, "equalitor is null");
        this.clusteredInput = requireNonNull(clusteredInput, "clusteredInput is null");
    }

    @Override
    public ProcessorState addInput(@Nullable Page input)
    {
        if (input == null) {
            return clusteredInput.addInput(null);
        }
        else {
            if (lastPage == null) {
                lastPage = input;
                return clusteredInput.addInput(MultiPageChunkCursor.initial(input, equalitor));
            }
            else {
                MultiPageChunkCursor chunkCursor = MultiPageChunkCursor.next(input, equalitor, lastPage);
                lastPage = input;
                return clusteredInput.addInput(chunkCursor);
            }
        }
    }
}
