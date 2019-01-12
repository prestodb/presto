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

public interface LookupSourceProvider
        extends AutoCloseable
{
    <R> R withLease(Function<LookupSourceLease, R> action);

    @Override
    void close();

    interface LookupSourceLease
    {
        LookupSource getLookupSource();

        boolean hasSpilled();

        long spillEpoch();

        IntPredicate getSpillMask();
    }
}
