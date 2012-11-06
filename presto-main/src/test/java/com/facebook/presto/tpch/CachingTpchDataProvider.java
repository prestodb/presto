package com.facebook.presto.tpch;

import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.base.Preconditions;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CachingTpchDataProvider
    implements TpchDataProvider
{
    private final Map<TpchColumnRequest, File> localFileCache = new HashMap<>();
    private final TpchDataProvider delegate;

    public CachingTpchDataProvider(TpchDataProvider delegate)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
    }

    @Override
    public File getColumnFile(Column column, BlocksFileEncoding encoding)
    {
        Preconditions.checkNotNull(column, "column is null");
        Preconditions.checkNotNull(encoding, "encoding is null");

        // Hack: Use the serdeName as the unique identifier of the serializer
        TpchColumnRequest columnRequest = new TpchColumnRequest(column, encoding.getName());
        File file = localFileCache.get(columnRequest);
        if (file == null) {
            file = delegate.getColumnFile(column, encoding);
            localFileCache.put(columnRequest, file);
        }
        return file;
    }

    private static final class TpchColumnRequest
    {
        private final TpchSchema.Column column;
        private final String serdeName;

        private TpchColumnRequest(TpchSchema.Column column, String serdeName)
        {
            this.column = checkNotNull(column, "column is null");
            this.serdeName = checkNotNull(serdeName, "serdeName is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof TpchColumnRequest)) return false;

            TpchColumnRequest that = (TpchColumnRequest) o;

            if (!column.equals(that.column)) return false;
            if (!serdeName.equals(that.serdeName)) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = column.hashCode();
            result = 31 * result + serdeName.hashCode();
            return result;
        }
    }
}
