package com.facebook.presto.tpch;

import com.facebook.presto.serde.BlocksFileEncoding;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CachingTpchDataFileLoader
    implements TpchDataFileLoader
{
    private final Map<TpchColumnRequest, File> localFileCache = new HashMap<>();
    private final TpchDataFileLoader delegate;

    public CachingTpchDataFileLoader(TpchDataFileLoader delegate)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
    }

    @Override
    public File getDataFile(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(encoding, "encoding is null");

        // Hack: Use the serdeName as the unique identifier of the serializer
        TpchColumnRequest columnRequest = new TpchColumnRequest(tableHandle, columnHandle, encoding.getName());
        File file = localFileCache.get(columnRequest);
        if (file == null) {
            file = delegate.getDataFile(tableHandle, columnHandle, encoding);
            localFileCache.put(columnRequest, file);
        }
        return file;
    }

    private static final class TpchColumnRequest
    {
        private final TpchTableHandle tableHandle;
        private final TpchColumnHandle columnHandle;
        private final String serdeName;

        private TpchColumnRequest(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, String serdeName)
        {
            this.tableHandle =  checkNotNull(tableHandle, "tableHandle is null");
            this.columnHandle =  checkNotNull(columnHandle, "columnHandle is null");
            this.serdeName =  checkNotNull(serdeName, "serdeName is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TpchColumnRequest)) {
                return false;
            }

            TpchColumnRequest that = (TpchColumnRequest) o;

            if (!columnHandle.equals(that.columnHandle)) {
                return false;
            }
            if (!serdeName.equals(that.serdeName)) {
                return false;
            }
            if (!tableHandle.equals(that.tableHandle)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = tableHandle.hashCode();
            result = 31 * result + columnHandle.hashCode();
            result = 31 * result + serdeName.hashCode();
            return result;
        }
    }
}
