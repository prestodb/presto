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
package io.prestosql.spiller;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.ExceededSpillLimitException;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.ExceededSpillLimitException.exceededLocalLimit;
import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SpillSpaceTracker
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    public SpillSpaceTracker(DataSize maxSize)
    {
        requireNonNull(maxSize, "maxSize is null");
        maxBytes = maxSize.toBytes();
        currentBytes = 0;
    }

    /**
     * Reserves the given number of bytes to spill. If more than the maximum, throws an exception.
     *
     * @throws ExceededSpillLimitException
     */
    public synchronized ListenableFuture<?> reserve(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if ((currentBytes + bytes) >= maxBytes) {
            throw exceededLocalLimit(succinctBytes(maxBytes));
        }
        currentBytes += bytes;

        return NOT_BLOCKED;
    }

    public synchronized void free(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(currentBytes - bytes >= 0, "tried to free more disk space than is reserved");
        currentBytes -= bytes;
    }

    /**
     * Returns the number of bytes currently on disk.
     */
    @Managed
    public synchronized long getCurrentBytes()
    {
        return currentBytes;
    }

    @Managed
    public synchronized long getMaxBytes()
    {
        return maxBytes;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("maxBytes", maxBytes)
                .add("currentBytes", currentBytes)
                .toString();
    }
}
