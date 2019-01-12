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
package io.prestosql.operator;

import java.util.function.Function;
import java.util.function.IntPredicate;

import static java.util.Objects.requireNonNull;

public final class StaticLookupSourceProvider
        implements LookupSourceProvider
{
    private final LookupSource lookupSource;

    public StaticLookupSourceProvider(LookupSource lookupSource)
    {
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
    }

    @Override
    public <R> R withLease(Function<LookupSourceLease, R> action)
    {
        return action.apply(new SimpleLookupSourceLease());
    }

    @Override
    public void close()
    {
        lookupSource.close();
    }

    private class SimpleLookupSourceLease
            implements LookupSourceLease
    {
        @Override
        public LookupSource getLookupSource()
        {
            return lookupSource;
        }

        @Override
        public boolean hasSpilled()
        {
            return false;
        }

        @Override
        public long spillEpoch()
        {
            return 0;
        }

        @Override
        public IntPredicate getSpillMask()
        {
            return i -> false;
        }
    }
}
