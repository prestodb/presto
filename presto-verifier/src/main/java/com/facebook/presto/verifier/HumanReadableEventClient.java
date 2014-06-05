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
package com.facebook.presto.verifier;

import com.facebook.presto.util.Types;
import io.airlift.event.client.AbstractEventClient;

import java.io.IOException;
import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkNotNull;

public class HumanReadableEventClient
        extends AbstractEventClient
{
    private final PrintStream out;
    private final boolean alwaysPrint;

    public HumanReadableEventClient(PrintStream out, boolean alwaysPrint)
    {
        this.out = checkNotNull(out, "out is null");
        this.alwaysPrint = alwaysPrint;
    }

    @Override
    public <T> void postEvent(T event)
            throws IOException
    {
        VerifierQueryEvent queryEvent = Types.checkType((Object) event, VerifierQueryEvent.class, "event");

        if (alwaysPrint || queryEvent.isFailed()) {
            printEvent(queryEvent);
        }
    }

    private void printEvent(VerifierQueryEvent queryEvent)
    {
        out.println("----------");
        out.println("Name: " + queryEvent.getName());
        out.println("Schema (control): " + queryEvent.getControlSchema());
        out.println("Schema (test): " + queryEvent.getTestSchema());
        out.println("Valid: " + !queryEvent.isFailed());
        out.println("Query (test): " + queryEvent.getTestQuery());

        if (queryEvent.isFailed()) {
            out.println("\nError message:\n" + queryEvent.getErrorMessage());
        }
        else {
            out.println("Control Duration (secs): " + queryEvent.getControlWallTimeSecs());
            out.println("   Test Duration (secs): " + queryEvent.getTestWallTimeSecs());
        }
        out.println("----------");
    }
}
