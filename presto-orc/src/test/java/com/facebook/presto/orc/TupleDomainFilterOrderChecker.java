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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TupleDomainFilterOrderChecker
{
    private final List<Integer> expectedOrder;
    private final List<Integer> actualOrder = new ArrayList<>();

    public TupleDomainFilterOrderChecker(List<Integer> expectedOrder)
    {
        this.expectedOrder = requireNonNull(expectedOrder, "expectedOrder is null");
    }

    public void call(int column)
    {
        if (!actualOrder.contains(column)) {
            actualOrder.add(column);
        }
    }

    public void assertOrder()
    {
        assertEquals(actualOrder, expectedOrder, "Actual order " + actualOrder + " doesn't match desired order " + expectedOrder);
    }
}
