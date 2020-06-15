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

import java.util.Iterator;

import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static java.util.Objects.requireNonNull;

public class PrestoSparkSerializedPageInput
        implements PrestoSparkPageInput
{
    private final PagesSerde pagesSerde;
    private final Iterator<PrestoSparkSerializedPage> serializedPageIterator;

    public PrestoSparkSerializedPageInput(PagesSerde pagesSerde, Iterator<PrestoSparkSerializedPage> serializedPageIterator)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.serializedPageIterator = requireNonNull(serializedPageIterator, "serializedPageIterator is null");
    }

    @Override
    public Page getNextPage()
    {
        PrestoSparkSerializedPage prestoSparkSerializedPage;
        synchronized (serializedPageIterator) {
            if (!serializedPageIterator.hasNext()) {
                return null;
            }
            prestoSparkSerializedPage = serializedPageIterator.next();
        }
        return pagesSerde.deserialize(toSerializedPage(prestoSparkSerializedPage));
    }
}
