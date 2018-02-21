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
package com.facebook.presto.operator;

import com.facebook.presto.operator.ContinuousWorkUtils.WorkState;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ContinuousWork<T>
{
    /**
     * Call the method to do the work.
     * The caller can keep calling this method until it returns true.
     * Result if ready when {@link ContinuousWork#process()} returns
     * true and the work hasn't finished. After obtaining the result
     * the work can be continued again by calling {@link ContinuousWork#process()}.
     */
    boolean process();

    boolean isBlocked();

    /**
     * @return a blocked future when {@link ContinuousWork#isBlocked()} returned true.
     */
    ListenableFuture<?> getBlockedFuture();

    /**
     * @return true if the continuous work is finished. No more results are expected.
     */
    boolean isFinished();

    /**
     * Get the result once the unit of work is done and the work hasn't finished.
     */
    T getResult();

    default <R> ContinuousWork<R> flatMap(Function<T, Iterator<R>> mapper)
    {
        return ContinuousWorkUtils.flatMap(this, mapper);
    }

    default <R> ContinuousWork<R> map(Function<T, R> mapper)
    {
        return ContinuousWorkUtils.map(this, mapper);
    }

    default <R> ContinuousWork<R> transform(Function<Optional<T>, WorkState<R>> transformation)
    {
        return ContinuousWorkUtils.transform(this, transformation);
    }

    static <T> ContinuousWork<T> fromIterable(Iterable<T> iterable)
    {
        return ContinuousWorkUtils.fromIterator(iterable.iterator());
    }

    static <T> ContinuousWork<T> fromIterator(Iterator<T> iterator)
    {
        return ContinuousWorkUtils.fromIterator(iterator);
    }

    static <T> ContinuousWork<T> create(Supplier<WorkState<T>> supplier)
    {
        return ContinuousWorkUtils.create(supplier);
    }

    static <T> ContinuousWork<T> mergeSorted(Iterable<ContinuousWork<T>> workIterable, Comparator<T> comparator)
    {
        return ContinuousWorkUtils.mergeSorted(workIterable, comparator);
    }
}
