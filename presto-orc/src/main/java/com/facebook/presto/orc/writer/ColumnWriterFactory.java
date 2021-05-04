package com.facebook.presto.orc.writer;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfEncryptionInfo;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import org.joda.time.DateTimeZone;

import java.util.List;

import static com.facebook.presto.orc.writer.ColumnWriters.createColumnWriter;

public class ColumnWriterFactory
{
    private int columnIndex;
    private List<OrcType> orcTypes;
    private Type type;
    private ColumnWriterOptions columnWriterOptions;
    private OrcEncoding orcEncoding;
    private DateTimeZone hiveStorageTimeZone;
    private DwrfEncryptionInfo dwrfEncryptors;
    private MetadataWriter metadataWriter;

    ColumnWriterFactory setColumnIndex(int columnIndex)
    {
        this.columnIndex = columnIndex;
        return this;
    }

    ColumnWriterFactory setOrcTypes(List<OrcType> orcTypes)
    {
        this.orcTypes = orcTypes;
        return this;
    }

    ColumnWriterFactory setType(Type type)
    {
        this.type = type;
        return this;
    }

    ColumnWriterFactory setColumnWriterOptions(ColumnWriterOptions columnWriterOptions)
    {
        this.columnWriterOptions = columnWriterOptions;
        return this;
    }

    ColumnWriterFactory setOrcEncoding(OrcEncoding orcEncoding)
    {
        this.orcEncoding = orcEncoding;
        return this;
    }

    ColumnWriterFactory setHiveStorageTimeZone(DateTimeZone hiveStorageTimeZone)
    {
        this.hiveStorageTimeZone = hiveStorageTimeZone;
        return this;
    }


    ColumnWriterFactory setDwrfEncryptors(DwrfEncryptionInfo dwrfEncryptors)
    {
        this.dwrfEncryptors = dwrfEncryptors;
        return this;
    }

    ColumnWriterFactory setMetadataWriter(MetadataWriter metadataWriter)
    {
        this.metadataWriter = metadataWriter;
        return this;
    }

    
    ColumnWriter getColumnWriter(int nodeSequence) {
        ColumnWriter columnWriter = createColumnWriter(
            columnIndex,
            orcTypes,
            type,
            columnWriterOptions,
            orcEncoding,
            hiveStorageTimeZone,
            dwrfEncryptors,
            metadataWriter);
        return columnWriter;
    }
}
