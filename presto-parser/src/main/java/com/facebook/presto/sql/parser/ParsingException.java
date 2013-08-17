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
