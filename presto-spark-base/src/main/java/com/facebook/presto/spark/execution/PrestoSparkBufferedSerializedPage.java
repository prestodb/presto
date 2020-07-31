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

import com.facebook.presto.spi.page.SerializedPage;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class PrestoSparkBufferedSerializedPage
        implements PrestoSparkBufferedResult
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PrestoSparkBufferedSerializedPage.class).instanceSize();

    private final SerializedPage serializedPage;

    public PrestoSparkBufferedSerializedPage(SerializedPage serializedPage)
    {
        this.serializedPage = requireNonNull(serializedPage, "serializedPage is null");
    }

    public SerializedPage getSerializedPage()
    {
        return serializedPage;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + serializedPage.getRetainedSizeInBytes();
    }

    @Override
    public int getPositionCount()
    {
        return serializedPage.getPositionCount();
    }
}
