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
package com.facebook.presto.cli;

import jline.internal.NonBlockingInputStream;

import java.io.IOException;

import static org.fusesource.jansi.internal.CLibrary.STDIN_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public final class KeyReader
{
    private KeyReader() {}

    @SuppressWarnings("resource")
    public static int peekKey(LineReader reader)
    {
        if (!hasTerminal()) {
            return -1;
        }

        try {
            return ((NonBlockingInputStream) reader.getInput()).peek(500);
        }
        catch (IOException e) {
            // ignore errors reading keyboard input
        }
        return -1;
    }

    /**
     * This method should be called after peekKey to consume the
     * byte that was returned in that call
     */
    @SuppressWarnings("resource")
    public static int readKey(LineReader reader)
    {
        if (!hasTerminal()) {
            return -1;
        }

        try {
            return reader.getInput().read();
        }
        catch (IOException e) {
            // ignore errors reading keyboard input
        }
        return -1;
    }

    private static boolean hasTerminal()
    {
        try {
            return isatty(STDIN_FILENO) == 1;
        }
        catch (Throwable e) {
            return false;
        }
    }
}
