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

import javax.inject.Inject;

import java.io.IOException;

import static java.lang.System.out;

public class HumanReadableEventClient
        extends AbstractEventClient
{
    private final boolean alwaysPrint;

    @Inject
    public HumanReadableEventClient(VerifierConfig config)
    {
        this.alwaysPrint = config.isAlwaysReport();
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

    private static void printEvent(VerifierQueryEvent queryEvent)
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
