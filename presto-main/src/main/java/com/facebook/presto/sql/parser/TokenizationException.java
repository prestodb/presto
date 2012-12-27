package com.facebook.presto.sql.parser;

import org.antlr.runtime.RecognitionException;

public class TokenizationException
        extends RuntimeException
{
    public TokenizationException(RecognitionException cause)
    {
        super(cause);
    }

    @Override
    public synchronized RecognitionException getCause()
    {
        return (RecognitionException) super.getCause();
    }
}
