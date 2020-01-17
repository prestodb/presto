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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.presto.verifier.framework.VerifierConfig;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.PrintStream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HumanReadableEventClient
        extends AbstractEventClient
{
    private final PrintStream out;

    @Inject
    public HumanReadableEventClient(VerifierConfig config)
            throws FileNotFoundException
    {
        requireNonNull(config.getHumanReadableEventLogFile(), "humanReadableEventLogFile is null");
        this.out = config.getHumanReadableEventLogFile().isPresent() ? new PrintStream(config.getHumanReadableEventLogFile().get()) : System.out;
    }

    @Override
    protected <T> void postEvent(T event)
    {
        VerifierQueryEvent queryEvent = (VerifierQueryEvent) event;
        out.println(format("%s: %s", queryEvent.getName(), queryEvent.getStatus()));
    }
}
