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
package com.facebook.presto.sql.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;

import static java.lang.String.format;

public class ParsingException
        extends RuntimeException
{
    public ParsingException(String message, RecognitionException cause)
    {
        super(message, cause);
    }

    public ParsingException(String message)
    {
        this(message, new RecognitionException(new ANTLRStringStream()));
    }

    @Override
    public RecognitionException getCause()
    {
        return (RecognitionException) super.getCause();
    }

    public int getLineNumber()
    {
        return getCause().line;
    }

    public int getColumnNumber()
    {
        return getCause().charPositionInLine + 1;
    }

    public String getErrorMessage()
    {
        return super.getMessage();
    }

    @Override
    public String getMessage()
    {
        return format("line %s:%s: %s", getLineNumber(), getColumnNumber(), getErrorMessage());
    }
}
