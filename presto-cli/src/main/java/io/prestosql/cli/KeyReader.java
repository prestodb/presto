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

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.fusesource.jansi.internal.CLibrary.STDIN_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public final class KeyReader
{
    private KeyReader() {}

    @SuppressWarnings("resource")
    public static int readKey()
    {
        if (!hasTerminal()) {
            return -1;
        }

        try {
            InputStream in = new FileInputStream(FileDescriptor.in);
            if (in.available() > 0) {
                return in.read();
            }
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
