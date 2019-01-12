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

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;

class ThreadInterruptor
        implements Closeable
{
    private final Thread thread = Thread.currentThread();

    @GuardedBy("this")
    private boolean processing = true;

    public synchronized void interrupt()
    {
        if (processing) {
            thread.interrupt();
        }
    }

    @Override
    public synchronized void close()
    {
        processing = false;
        Thread.interrupted();
    }
}
