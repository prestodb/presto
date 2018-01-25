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

package com.facebook.presto.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.FutureStateChange.StateChangeFuture.getFirstCompleteAndCancelOthers;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestFutureStateChange
{
    public void testComplete()
    {


    }

    @Test
    public void testGetFirstCompleteAndCancelOthers()
            throws Exception
    {
        FutureStateChange<Integer> stateChange1 = new FutureStateChange<>();
        FutureStateChange<Integer> stateChange2 = new FutureStateChange<>();
        FutureStateChange<Integer> stateChange3 = new FutureStateChange<>();

        ListenableFuture<Integer> future = getFirstCompleteAndCancelOthers(
                ImmutableList.of(stateChange1.newStateChangeFuture(), stateChange2.newStateChangeFuture(), stateChange3.newStateChangeFuture()));

        assertEquals(stateChange1.getListenerCount(), 1);
        assertEquals(stateChange2.getListenerCount(), 1);
        assertEquals(stateChange3.getListenerCount(), 1);

        assertFalse(future.isDone());
        stateChange2.complete(100);
        assertEquals((int) future.get(), 100);

        assertEquals(stateChange1.getListenerCount(), 0);
        assertEquals(stateChange2.getListenerCount(), 0);
        assertEquals(stateChange3.getListenerCount(), 0);
    }

    @Test
    public void testGetFirstCompleteAndCancelOthersWithCompletedListener()
            throws Exception
    {
        FutureStateChange<Integer> stateChange1 = new FutureStateChange<>();
        FutureStateChange<Integer> stateChange2 = new FutureStateChange<>();
        FutureStateChange<Integer> stateChange3 = new FutureStateChange<>();

        FutureStateChange.StateChangeFuture<Integer> listener1 = stateChange1.newStateChangeFuture();
        FutureStateChange.StateChangeFuture<Integer> listener2 = stateChange2.newStateChangeFuture();
        FutureStateChange.StateChangeFuture<Integer> listener3 = stateChange3.newStateChangeFuture();

        stateChange2.complete(100);

        ListenableFuture<Integer> future = getFirstCompleteAndCancelOthers(
                ImmutableList.of(listener1, listener2, listener3));

        assertEquals((int) future.get(), 100);

        assertEquals(stateChange1.getListenerCount(), 0);
        assertEquals(stateChange2.getListenerCount(), 0);
        assertEquals(stateChange3.getListenerCount(), 0);
    }
}
