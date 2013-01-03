package com.facebook.presto.event.scribe.client;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Arrays;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
@ThriftStruct
public class LogEntry
{
    private final byte[] category;
    private final byte[] message;
    private final SourceInfo sourceInfo;
    private final Integer bucket;
    private final Integer checksum;

    @ThriftConstructor
    public LogEntry(byte[] category, byte[] message, @Nullable Integer checksum, @Nullable SourceInfo sourceInfo, @Nullable Integer bucket)
    {
        checkNotNull(category, "category is null");
        checkNotNull(message, "message is null");

        this.category = Arrays.copyOf(category, category.length);
        this.message = Arrays.copyOf(message, message.length);
        this.checksum = checksum;
        this.sourceInfo = sourceInfo;
        this.bucket = bucket;
    }

    public LogEntry(byte[] category, byte[] message, @Nullable SourceInfo sourceInfo, @Nullable Integer bucket)
    {
        this(category, message, computeChecksum(category, message), sourceInfo, bucket);
    }

    public LogEntry(byte[] category, byte[] message, @Nullable Integer bucket)
    {
        this(category, message, computeChecksum(category, message), null, bucket);
    }

    public LogEntry(byte[] category, byte[] message)
    {
        this(category, message, computeChecksum(category, message), null, null);
    }

    public LogEntry(String category, String message)
    {
        this(category.getBytes(Charsets.UTF_8), message.getBytes(Charsets.UTF_8));
    }

    @ThriftField(1)
    public byte[] getCategory()
    {
        return category;
    }

    @ThriftField(2)
    public byte[] getMessage()
    {
        return message;
    }

    @ThriftField(3)
    public SourceInfo getSourceInfo()
    {
        return sourceInfo;
    }

    @ThriftField(4)
    public Integer getChecksum()
    {
        return checksum;
    }

    @ThriftField(5)
    public Integer getBucket()
    {
        return bucket;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogEntry logEntry = (LogEntry) o;

        if (bucket != null ? !bucket.equals(logEntry.bucket) : logEntry.bucket != null) {
            return false;
        }
        if (!Arrays.equals(category, logEntry.category)) {
            return false;
        }
        if (checksum != null ? !checksum.equals(logEntry.checksum) : logEntry.checksum != null) {
            return false;
        }
        if (!Arrays.equals(message, logEntry.message)) {
            return false;
        }
        if (sourceInfo != null ? !sourceInfo.equals(logEntry.sourceInfo) : logEntry.sourceInfo != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode(category);
        result = 31 * result + Arrays.hashCode(message);
        result = 31 * result + (sourceInfo != null ? sourceInfo.hashCode() : 0);
        result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
        result = 31 * result + (checksum != null ? checksum.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("category", category)
                .add("message", message)
                .add("sourceInfo", sourceInfo)
                .add("bucket", bucket)
                .add("checksum", checksum)
                .toString();
    }

    // Needs to match logic in com.facebook.datafreeway.integrity.LogEntryCheckSum
    private static int computeChecksum(byte[] category, byte[] message)
    {
        CRC32 crc = new CRC32();

        crc.update(category);
        crc.update(message);

        //noinspection NumericCastThatLosesPrecision
        return (int) crc.getValue();
    }
}
