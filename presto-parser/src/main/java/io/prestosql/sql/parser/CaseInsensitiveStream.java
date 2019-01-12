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
package io.prestosql.sql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

public class CaseInsensitiveStream
        implements CharStream
{
    private final CharStream stream;

    public CaseInsensitiveStream(CharStream stream)
    {
        this.stream = stream;
    }

    @Override
    public String getText(Interval interval)
    {
        return stream.getText(interval);
    }

    @Override
    public void consume()
    {
        stream.consume();
    }

    @Override
    public int LA(int i)
    {
        int result = stream.LA(i);

        switch (result) {
            case 0:
            case IntStream.EOF:
                return result;
            default:
                return Character.toUpperCase(result);
        }
    }

    @Override
    public int mark()
    {
        return stream.mark();
    }

    @Override
    public void release(int marker)
    {
        stream.release(marker);
    }

    @Override
    public int index()
    {
        return stream.index();
    }

    @Override
    public void seek(int index)
    {
        stream.seek(index);
    }

    @Override
    public int size()
    {
        return stream.size();
    }

    @Override
    public String getSourceName()
    {
        return stream.getSourceName();
    }
}
