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
package com.facebook.presto.tests.cli;

import io.prestodb.tempto.process.LocalCliProcess;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public final class PrestoCliProcess
        extends LocalCliProcess
{
    private static final Pattern PRESTO_PROMPT_PATTERN = Pattern.compile("presto(:[a-z0-9_]+)?>");

    public PrestoCliProcess(Process process)
    {
        super(process);
    }

    public List<String> readLinesUntilPrompt()
    {
        List<String> lines = new ArrayList<>();
        while (!hasNextOutput(PRESTO_PROMPT_PATTERN)) {
            lines.add(nextOutputLine());
        }
        waitForPrompt();
        return lines;
    }

    public void waitForPrompt()
    {
        assertThat(nextOutputToken()).matches(PRESTO_PROMPT_PATTERN);
    }
}
