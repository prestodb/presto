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

import com.facebook.presto.execution.buffer.SerializedPage;

import static java.util.Objects.requireNonNull;

public class PageWithPartition
{
    private final SerializedPage page;
    private final int partition;

    public PageWithPartition(SerializedPage page, int partition)
    {
        this.page = requireNonNull(page, "page is null");
        this.partition = partition;
    }

    public SerializedPage getPage()
    {
        return page;
    }

    public int getPartition()
    {
        return partition;
    }
}
