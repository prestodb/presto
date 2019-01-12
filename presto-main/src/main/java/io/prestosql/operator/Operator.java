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
package io.prestosql.operator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;

public interface Operator
        extends AutoCloseable
{
    ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    OperatorContext getOperatorContext();

    /**
     * Returns a future that will be completed when the operator becomes
     * unblocked.  If the operator is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    default ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    /**
     * Returns true if and only if this operator can accept an input page.
     */
    boolean needsInput();

    /**
     * Adds an input page to the operator.  This method will only be called if
     * {@code needsInput()} returns true.
     */
    void addInput(Page page);

    /**
     * Gets an output page from the operator.  If no output data is currently
     * available, return null.
     */
    Page getOutput();

    /**
     * After calling this method operator should revoke all reserved revocable memory.
     * As soon as memory is revoked returned future should be marked as done.
     * <p>
     * Spawned threads can not modify OperatorContext because it's not thread safe.
     * For this purpose implement {@link #finishMemoryRevoke()}
     * <p>
     * Since memory revoking signal is delivered asynchronously to the Operator, implementation
     * must gracefully handle the case when there no longer is any revocable memory allocated.
     * <p>
     * After this method is called on Operator the Driver is disallowed to call any
     * processing methods on it (isBlocked/needsInput/addInput/getOutput) until
     * {@link #finishMemoryRevoke()} is called.
     */
    default ListenableFuture<?> startMemoryRevoke()
    {
        return NOT_BLOCKED;
    }

    /**
     * Clean up and release resources after completed memory revoking. Called by driver
     * once future returned by startMemoryRevoke is completed.
     */
    default void finishMemoryRevoke()
    {
    }

    /**
     * Notifies the operator that no more pages will be added and the
     * operator should finish processing and flush results. This method
     * will not be called if the Task is already failed or canceled.
     */
    void finish();

    /**
     * Is this operator completely finished processing and no more
     * output pages will be produced.
     */
    boolean isFinished();

    /**
     * This method will always be called before releasing the Operator reference.
     */
    @Override
    default void close()
            throws Exception
    {
    }
}
