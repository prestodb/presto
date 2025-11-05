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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.Page;
import jakarta.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * The result of processing input by {@link TableFunctionDataProcessor} or {@link TableFunctionSplitProcessor}.
 * It can optionally include a portion of output data in the form of {@link Page}
 * The returned {@link Page} should consist of:
 * - proper columns produced by the table function
 * - one column of type {@code BIGINT} for each table function's input table having the pass-through property (see {@link TableArgumentSpecification#isPassThroughColumns}),
 * in order of the corresponding argument specifications. Entries in these columns are the indexes of input rows (from partition start) to be attached to output,
 * or null to indicate that a row of nulls should be attached instead of an input row. The indexes are validated to be within the portion of the partition
 * provided to the function so far.
 * Note: when the input is empty, the only valid index value is null, because there are no input rows that could be attached to output. In such case, for performance
 * reasons, the validation of indexes is skipped, and all pass-through columns are filled with nulls.
 */
public interface TableFunctionProcessorState
{
    final class Blocked
            implements TableFunctionProcessorState
    {
        private final CompletableFuture<Void> future;

        private Blocked(CompletableFuture<Void> future)
        {
            this.future = requireNonNull(future, "future is null");
        }

        public static Blocked blocked(CompletableFuture<Void> future)
        {
            return new Blocked(future);
        }

        public CompletableFuture<Void> getFuture()
        {
            return future;
        }
    }

    final class Finished
            implements TableFunctionProcessorState
    {
        public static final Finished FINISHED = new Finished();

        private Finished() {}
    }

    final class Processed
            implements TableFunctionProcessorState
    {
        private final boolean usedInput;
        private final Page result;

        private Processed(boolean usedInput, @Nullable Page result)
        {
            this.usedInput = usedInput;
            this.result = result;
        }

        public static Processed usedInput()
        {
            return new Processed(true, null);
        }

        public static Processed produced(Page result)
        {
            requireNonNull(result, "result is null");
            return new Processed(false, result);
        }

        public static Processed usedInputAndProduced(Page result)
        {
            requireNonNull(result, "result is null");
            return new Processed(true, result);
        }

        public boolean isUsedInput()
        {
            return usedInput;
        }

        public Page getResult()
        {
            return result;
        }
    }
}
