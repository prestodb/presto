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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.Page;

import java.util.List;

public interface StoragePageSink
{
    void appendPages(List<Page> pages);

    void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes);

    boolean isFull();

    void flush();

    List<ShardInfo> commit();

    void rollback();
}
