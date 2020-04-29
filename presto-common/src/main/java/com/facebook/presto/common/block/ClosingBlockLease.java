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
package com.facebook.presto.common.block;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public final class ClosingBlockLease
        implements BlockLease
{
    private final Block block;
    private final List<Closer> closers;
    private boolean closed;
    private boolean retrieved;

    public ClosingBlockLease(Block block, Closer... closers)
    {
        requireNonNull(block, "block is null");
        requireNonNull(closers, "closers is null");
        this.block = block;
        this.closers = closers.length == 0 ? emptyList() : unmodifiableList(asList(closers.clone()));
    }

    public static BlockLease newLease(Block block, Closer... closers)
    {
        return new ClosingBlockLease(block, closers);
    }

    @Override
    public Block get()
    {
        checkState(!closed, "block lease is closed");
        checkState(!retrieved, "block lease already retrieved");
        retrieved = true;
        return block;
    }

    // Implementations of Closer should not throw, but in theory they could.  If they do,
    // make a best-effort attempt to preserve the exception history and close all resources.
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        Throwable innerException = null;
        for (Closer closer : closers) {
            try {
                closer.close();
            }
            catch (Throwable t) {
                if (innerException == null) {
                    innerException = t;
                }
                else if (innerException != t) {
                    // Self-suppression not permitted
                    innerException.addSuppressed(t);
                }
            }
        }
        if (innerException != null) {
            // Convert to unchecked exception
            throw new RuntimeException(innerException);
        }
    }

    private static void checkState(boolean state, String message)
    {
        if (!state) {
            throw new IllegalStateException(message);
        }
    }

    // Implementations of Closer should not throw
    public interface Closer
            extends AutoCloseable
    {
        @Override
        void close();
    }
}
