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
package com.facebook.presto.framework;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class ConnectorTestFramework
{
    private Supplier<Connector> widgetSupplier;
    private static Map<Supplier<Connector>, AtomicInteger> connectorTestCounts = new HashMap<>();

    protected ConnectorTestFramework(ConnectorTestHelper helper, int connectorTestCount)
    {
        this.widgetSupplier = requireNonNull(helper, "widgetSupplier is null").getSupplier();
        connectorTestCounts.computeIfAbsent(widgetSupplier, key -> new AtomicInteger(connectorTestCount));
    }

    @AfterClass
    public void close()
    {
        connectorTestCounts.get(widgetSupplier).decrementAndGet();
    }

    @AfterSuite
    public void verifyCounts()
    {
        connectorTestCounts.forEach((key, value) -> assertEquals(value.get(), 0));
    }

    protected Connector get()
    {
        /*
         * In a full-blown implementation, this would handle some caching so
         * that expensive QueryRunners don't get recreated for every test
         * class. This is a stub implementation to prove out the code generation
         * concept.
         */
        return widgetSupplier.get();
    }
}
