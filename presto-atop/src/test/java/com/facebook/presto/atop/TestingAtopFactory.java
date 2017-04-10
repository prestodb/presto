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
package com.facebook.presto.atop;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingAtopFactory
        implements AtopFactory
{
    @Override
    public Atop create(AtopTable table, ZonedDateTime date)
    {
        InputStream data = TestingAtopFactory.class.getResourceAsStream(table.name().toLowerCase() + ".txt");
        requireNonNull(data, format("No data found for %s", table));
        return new TestingAtop(data, date);
    }

    private static final class TestingAtop
            implements Atop
    {
        private final BufferedReader reader;
        private final ZonedDateTime date;
        private String line;

        private TestingAtop(InputStream dataStream, ZonedDateTime date)
        {
            this.date = date;
            this.reader = new BufferedReader(new InputStreamReader(dataStream));
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                line = null;
            }
        }

        @Override
        public boolean hasNext()
        {
            return line != null;
        }

        @Override
        public String next()
        {
            if (line == null) {
                throw new NoSuchElementException();
            }
            String currentLine = line;
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                line = null;
            }

            if (currentLine.equals("SEP") || currentLine.equals("RESET")) {
                return currentLine;
            }

            List<String> fields = new ArrayList<>(Splitter.on(" ").splitToList(currentLine));

            // Timestamps in file are delta encoded
            long secondsSinceEpoch = date.toEpochSecond();
            secondsSinceEpoch += Integer.parseInt(fields.get(2));
            fields.set(2, String.valueOf(secondsSinceEpoch));

            return Joiner.on(" ").join(fields);
        }

        @Override
        public void close()
        {
            try {
                reader.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
