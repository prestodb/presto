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
package com.facebook.presto.spi;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface UpdatablePageSource
        extends ConnectorPageSource
{
    default void deleteRows(Block rowIds)
    {
        throw new UnsupportedOperationException("This connector does not support row-level delete");
    }

    default void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        throw new UnsupportedOperationException("This connector does not support row update");
    }

    CompletableFuture<Collection<Slice>> finish();

    default void abort() {}
}
