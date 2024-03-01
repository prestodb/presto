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
package com.facebook.presto.orc;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

/*
 * This class is a copy of HiveOrcLocalMemoryContext.
 * Any changes made to HiveOrcLocalMemoryContext should be made here as well
 */
public class TestingHiveOrcLocalMemoryContext
        implements OrcLocalMemoryContext
{
    private final LocalMemoryContext delegate;

    TestingHiveOrcLocalMemoryContext(LocalMemoryContext delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ListenableFuture<?> setBytes(long bytes)
    {
        return delegate.setBytes(bytes);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
