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

import com.facebook.presto.common.Page;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;

public interface GroupedTopNBuilder
{
    Work<?> processPage(Page page);

    Iterator<Page> buildResult();

    ListenableFuture<?> startMemoryRevoke();

    void finishMemoryRevoke();

    long getEstimatedSizeInBytes();

    long getGroupIdsSortingSize();

    GroupByHash getGroupByHash();
}
