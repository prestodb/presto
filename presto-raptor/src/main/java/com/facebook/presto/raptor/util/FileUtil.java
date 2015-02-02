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
package com.facebook.presto.raptor.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public final class FileUtil
{
    private FileUtil() {}

    /**
     * Copy a file and try to guarantee the copy is on disk.
     */
    public static void copyFile(Path source, Path target)
            throws IOException
    {
        try (FileChannel in = FileChannel.open(source, READ)) {
            long size = in.size();
            try (FileChannel out = FileChannel.open(target, WRITE, CREATE_NEW)) {
                long position = 0;
                while (position < size) {
                    position += in.transferTo(position, size - position, out);
                }
                out.force(false);
            }
        }
    }
}
