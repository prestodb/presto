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
package com.facebook.presto.druid.zip;

import javax.annotation.Nullable;

import java.util.EnumSet;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A full representation of a ZIP file entry.
 *
 * <p>See <a href="http://www.pkware.com/documents/casestudies/APPNOTE.TXT">ZIP Format</a> for
 * a description of the entry fields. (Section 4.3.7 and 4.4)
 */
public class ZipFileEntry
{
    /**
     * Compression method for ZIP entries.
     */
    public enum Compression
    {
        STORED((short) 0, Feature.STORED),
        DEFLATED((short) 8, Feature.DEFLATED);

        public static Compression fromValue(int value)
        {
            for (Compression c : Compression.values()) {
                if (c.getValue() == value) {
                    return c;
                }
            }
            return null;
        }

        private short value;
        private Feature feature;

        private Compression(short value, Feature feature)
        {
            this.value = value;
            this.feature = feature;
        }

        public short getValue()
        {
            return value;
        }

        public short getMinVersion()
        {
            return feature.getMinVersion();
        }

        Feature getFeature()
        {
            return feature;
        }
    }

    /**
     * General purpose bit flag for ZIP entries.
     */
    public enum Flag
    {
        DATA_DESCRIPTOR(3);

        private int bit;

        private Flag(int bit)
        {
            this.bit = bit;
        }

        public int getBit()
        {
            return bit;
        }
    }

    /**
     * Zip file features that entries may use.
     */
    enum Feature
    {
        DEFAULT((short) 0x0a),
        STORED((short) 0x0a),
        DEFLATED((short) 0x14),
        ZIP64_SIZE((short) 0x2d),
        ZIP64_CSIZE((short) 0x2d),
        ZIP64_OFFSET((short) 0x2d);

        private short minVersion;

        private Feature(short minVersion)
        {
            this.minVersion = minVersion;
        }

        public short getMinVersion()
        {
            return minVersion;
        }

        static short getMinRequiredVersion(EnumSet<Feature> featureSet)
        {
            short minVersion = Feature.DEFAULT.getMinVersion();
            for (Feature feature : featureSet) {
                minVersion = (short) Math.max(minVersion, feature.getMinVersion());
            }
            return minVersion;
        }
    }

    private String name;
    private long time = -1;
    private long crc = -1;
    private long size = -1;
    private long csize = -1;
    private Compression method;
    private short version = -1;
    private short versionNeeded = -1;
    private short flags;
    private short internalAttributes;
    private int externalAttributes;
    private long localHeaderOffset = -1;
    private ExtraDataList extra;
    @Nullable private String comment;

    private EnumSet<Feature> featureSet;

    public ZipFileEntry(String name)
    {
        this.featureSet = EnumSet.of(Feature.DEFAULT);
        setName(name);
        setMethod(Compression.STORED);
        setExtra(new ExtraDataList());
    }

    public ZipFileEntry(ZipFileEntry e)
    {
        this.name = e.getName();
        this.time = e.getTime();
        this.crc = e.getCrc();
        this.size = e.getSize();
        this.csize = e.getCompressedSize();
        this.method = e.getMethod();
        this.version = e.getVersion();
        this.versionNeeded = e.getVersionNeeded();
        this.flags = e.getFlags();
        this.internalAttributes = e.getInternalAttributes();
        this.externalAttributes = e.getExternalAttributes();
        this.localHeaderOffset = e.getLocalHeaderOffset();
        this.extra = new ExtraDataList(e.getExtra());
        this.comment = e.getComment();
        this.featureSet = EnumSet.copyOf(e.getFeatureSet());
    }

    public void setName(String name)
    {
        checkArgument(name != null, "Zip file name could not be null");
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public void setTime(long time)
    {
        this.time = time;
    }

    public long getTime()
    {
        return time;
    }

    public void setCrc(long crc)
    {
        if (crc < 0 || crc > 0xffffffffL) {
            throw new IllegalArgumentException("invalid entry crc-32");
        }
        this.crc = crc;
    }

    public long getCrc()
    {
        return crc;
    }

    public void setSize(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("invalid entry size");
        }
        if (size > 0xffffffffL) {
            featureSet.add(Feature.ZIP64_SIZE);
        }
        else {
            featureSet.remove(Feature.ZIP64_SIZE);
        }
        this.size = size;
    }

    public long getSize()
    {
        return size;
    }

    public void setCompressedSize(long csize)
    {
        if (csize < 0) {
            throw new IllegalArgumentException("invalid entry size");
        }
        if (csize > 0xffffffffL) {
            featureSet.add(Feature.ZIP64_CSIZE);
        }
        else {
            featureSet.remove(Feature.ZIP64_CSIZE);
        }
        this.csize = csize;
    }

    public long getCompressedSize()
    {
        return csize;
    }

    public void setMethod(Compression method)
    {
        checkArgument(method != null, "Zip file compression could not be null");
        if (this.method != null) {
            featureSet.remove(this.method.getFeature());
        }
        this.method = method;
        featureSet.add(this.method.getFeature());
    }

    public Compression getMethod()
    {
        return method;
    }

    public void setVersion(short version)
    {
        this.version = version;
    }

    public short getVersion()
    {
        return (short) Math.max(version, Feature.getMinRequiredVersion(featureSet));
    }

    public void setVersionNeeded(short versionNeeded)
    {
        this.versionNeeded = versionNeeded;
    }

    public short getVersionNeeded()
    {
        return (short) Math.max(versionNeeded, Feature.getMinRequiredVersion(featureSet));
    }

    public void setFlags(short flags)
    {
        this.flags = flags;
    }

    public void setFlag(Flag flag, boolean set)
    {
        short mask = 0x0000;
        mask |= 1 << flag.getBit();
        if (set) {
            flags |= mask;
        }
        else {
            flags &= ~mask;
        }
    }

    public short getFlags()
    {
        return flags;
    }

    public void setInternalAttributes(short internalAttributes)
    {
        this.internalAttributes = internalAttributes;
    }

    public short getInternalAttributes()
    {
        return internalAttributes;
    }

    /**
     * Sets the external file attributes of the entry.
     */
    public void setExternalAttributes(int externalAttributes)
    {
        this.externalAttributes = externalAttributes;
    }

    public int getExternalAttributes()
    {
        return externalAttributes;
    }

    void setLocalHeaderOffset(long localHeaderOffset)
    {
        if (localHeaderOffset < 0) {
            throw new IllegalArgumentException("invalid local header offset");
        }
        if (localHeaderOffset > 0xffffffffL) {
            featureSet.add(Feature.ZIP64_OFFSET);
        }
        else {
            featureSet.remove(Feature.ZIP64_OFFSET);
        }
        this.localHeaderOffset = localHeaderOffset;
    }

    public long getLocalHeaderOffset()
    {
        return localHeaderOffset;
    }

    public void setExtra(ExtraDataList extra)
    {
        checkArgument(extra != null, "Zip file data could not be null");
        if (extra.getLength() > 0xffff) {
            throw new IllegalArgumentException("invalid extra field length");
        }
        this.extra = extra;
    }

    public ExtraDataList getExtra()
    {
        return extra;
    }

    public void setComment(@Nullable String comment)
    {
        this.comment = comment;
    }

    public String getComment()
    {
        return comment;
    }

    EnumSet<Feature> getFeatureSet()
    {
        return featureSet;
    }

    @Override
    public String toString()
    {
        return "ZipFileEntry[" + name + "]";
    }
}
