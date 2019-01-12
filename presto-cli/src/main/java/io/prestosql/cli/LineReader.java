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
package io.prestosql.cli;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.history.History;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public class LineReader
        extends ConsoleReader
        implements Closeable
{
    private boolean interrupted;

    LineReader(History history, Completer... completers)
            throws IOException
    {
        setExpandEvents(false);
        setBellEnabled(true);
        setHandleUserInterrupt(true);
        setHistory(history);
        setHistoryEnabled(false);
        for (Completer completer : completers) {
            addCompleter(completer);
        }
    }

    @Override
    public String readLine(String prompt, Character mask)
            throws IOException
    {
        String line;
        interrupted = false;
        try {
            line = super.readLine(prompt, mask);
        }
        catch (UserInterruptException e) {
            interrupted = true;
            return null;
        }

        if (getHistory() instanceof Flushable) {
            ((Flushable) getHistory()).flush();
        }
        return line;
    }

    @Override
    public void close()
    {
        super.close();
    }

    public boolean interrupted()
    {
        return interrupted;
    }
}
