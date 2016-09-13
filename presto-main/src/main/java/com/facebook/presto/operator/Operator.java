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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface Operator
        extends AutoCloseable
{
    ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    OperatorContext getOperatorContext();

    /**
     * Gets the column types of pages produced by this operator.
     */
    List<Type> getTypes();

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
     */
    default ListenableFuture<?> revokeMemory()
    {
        return NOT_BLOCKED;
    }

    /**
     * This method will always be called before releasing the Operator reference.
     */
    @Override
    default void close()
            throws Exception
    {
    }
}
