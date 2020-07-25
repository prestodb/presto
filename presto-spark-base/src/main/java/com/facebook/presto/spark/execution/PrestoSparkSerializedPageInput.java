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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.Page;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spi.page.PagesSerde;

import javax.annotation.concurrent.GuardedBy;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static java.util.Objects.requireNonNull;

public class PrestoSparkSerializedPageInput
        implements PrestoSparkPageInput
{
    private final PagesSerde pagesSerde;
    @GuardedBy("this")
    private final List<Iterator<PrestoSparkSerializedPage>> serializedPageIterators;
    @GuardedBy("this")
    private int currentIteratorIndex;

    public PrestoSparkSerializedPageInput(PagesSerde pagesSerde, List<Iterator<PrestoSparkSerializedPage>> serializedPageIterators)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.serializedPageIterators = requireNonNull(serializedPageIterators, "serializedPageIterator is null");
    }

    @Override
    public Page getNextPage()
    {
        PrestoSparkSerializedPage prestoSparkSerializedPage = null;
        synchronized (this) {
            while (prestoSparkSerializedPage == null) {
                if (currentIteratorIndex >= serializedPageIterators.size()) {
                    return null;
                }
                Iterator<PrestoSparkSerializedPage> currentIterator = serializedPageIterators.get(currentIteratorIndex);
                if (currentIterator.hasNext()) {
                    prestoSparkSerializedPage = currentIterator.next();
                }
                else {
                    currentIteratorIndex++;
                }
            }
        }
        return pagesSerde.deserialize(toSerializedPage(prestoSparkSerializedPage));
    }
}
