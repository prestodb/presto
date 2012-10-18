package com.facebook.presto.sql.parser;

import org.antlr.runtime.CharStream;

public class CaseInsensitiveStream
        implements CharStream
{
    private CharStream stream;

    public CaseInsensitiveStream(CharStream stream)
    {
        this.stream = stream;
    }

    /**
     * @return the LA value without case transformation
     */
    public int rawLA(int i)
    {
        return stream.LA(i);
    }

    @Override
    public String substring(int start, int stop)
    {
        return stream.substring(start, stop);
    }

    @Override
    public int LT(int i)
    {
        return LA(i);
    }

    @Override
    public int getLine()
    {
        return stream.getLine();
    }

    @Override
    public void setLine(int line)
    {
        stream.setLine(line);
    }

    @Override
    public void setCharPositionInLine(int pos)
    {
        stream.setCharPositionInLine(pos);
    }

    @Override
    public int getCharPositionInLine()
    {
        return stream.getCharPositionInLine();
    }

    @Override
    public void consume()
    {
        stream.consume();
    }

    @Override
    public int LA(int i)
    {
        int result = stream.LT(i);

        switch (result) {
            case 0:
            case CharStream.EOF:
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
    public int index()
    {
        return stream.index();
    }

    @Override
    public void rewind(int marker)
    {
        stream.rewind(marker);
    }

    @Override
    public void rewind()
    {
        stream.rewind();
    }

    @Override
    public void release(int marker)
    {
        stream.release(marker);
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
