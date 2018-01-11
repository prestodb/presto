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
package com.facebook.presto.raptor.backup;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestNotifyingCounter
{
    @Test
    public void testCounter()
    {
        NotifyingCounter notifyingCounter = new NotifyingCounter(3);
        for (int i = 0; i < 2; ++i) {
            notifyingCounter.increment();
            assertTrue(notifyingCounter.getBelowThreshold().isDone());
        }

        notifyingCounter.increment();
        assertFalse(notifyingCounter.getBelowThreshold().isDone());

        notifyingCounter.decrement();
        assertTrue(notifyingCounter.getBelowThreshold().isDone());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "counter to decrement must > 0")
    public void testIllegalDecrement()
    {
        NotifyingCounter notifyingCounter = new NotifyingCounter(0);
        notifyingCounter.decrement();
    }
}
