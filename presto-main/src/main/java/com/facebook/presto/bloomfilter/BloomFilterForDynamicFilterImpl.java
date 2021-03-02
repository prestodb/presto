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
package com.facebook.presto.bloomfilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class BloomFilterForDynamicFilterImpl
        extends BloomFilterForDynamicFilter
        implements Serializable
{
    private BloomFilterImpl bloomFilter;
    private int sizeBeforeCompressed;
    private int sizeAfterCompressed;
    private byte[] compressedBloomFilter;

    @JsonCreator
    public BloomFilterForDynamicFilterImpl(
            @JsonProperty("bloomFilter") BloomFilterImpl bloomFilter,
            @JsonProperty("compressedBloomFilter") byte[] compressedBloomFilter,
            @JsonProperty("sizeBeforeCompressed") int sizeBeforeCompressed) throws IOException
    {
        this.bloomFilter = bloomFilter;

        decompressBloomFilter(compressedBloomFilter, sizeBeforeCompressed);
    }

    @JsonProperty
    public BloomFilterImpl getBloomFilter()
    {
        return bloomFilter;
    }

    @JsonProperty
    public byte[] getCompressedBloomFilter() throws IOException
    {
        compressBloomFilter();
        return compressedBloomFilter;
    }

    @JsonProperty
    public int getSizeBeforeCompressed()
    {
        return sizeBeforeCompressed;
    }

    @JsonProperty
    public int getSizeAfterCompressed()
    {
        return sizeAfterCompressed;
    }

    @JsonProperty
    public void setSizeAfterCompressed(int sizeAfterCompressed)
    {
        this.sizeAfterCompressed = sizeAfterCompressed;
    }

    @Override
    public void put(Object object)
    {
        this.bloomFilter.put(object);
    }

    @Override
    public boolean contain(Object object)
    {
        return this.bloomFilter.mightContain(object);
    }

    @Override
    public BloomFilterForDynamicFilter merge(BloomFilterForDynamicFilter bloomFilter)
    {
        if (bloomFilter.getClass() != BloomFilterForDynamicFilterImpl.class) {
            return this;
        }

        BloomFilterForDynamicFilterImpl apacheBloomFilter = (BloomFilterForDynamicFilterImpl) bloomFilter;
        if (this.bloomFilter == null || apacheBloomFilter.getBloomFilter() == null) {
            this.bloomFilter = null;
        }
        else {
            try {
                this.bloomFilter.mergeInPlace(apacheBloomFilter.getBloomFilter());
            }
            catch (IncompatibleMergeException e) {
                e.printStackTrace();
            }
        }

        return this;
    }

    @Override
    public void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException
    {
        inputStream.defaultReadObject();
        if (compressedBloomFilter != null) {
            synchronized (this) {
                byte[] uncompressed = Zstd.decompress(compressedBloomFilter, sizeBeforeCompressed);
                bloomFilter = BloomFilterImpl.readFrom(uncompressed);
            }
            compressedBloomFilter = null;
        }
    }

    @Override
    public void writeObject(ObjectOutputStream outputStream) throws IOException
    {
        compressBloomFilter();
        outputStream.defaultWriteObject();
    }

    private void compressBloomFilter() throws IOException
    {
        synchronized (this) {
            if (bloomFilter != null) {
                compressedBloomFilter = Zstd.compress(bloomFilter.toByteArray());
                sizeBeforeCompressed = bloomFilter.toByteArray().length;
                sizeAfterCompressed = compressedBloomFilter.length;
            }
        }
    }

    private void decompressBloomFilter(byte[] compressedBloomFilter, int sizeBeforeCompressed) throws IOException
    {
        if (compressedBloomFilter != null) {
            synchronized (this) {
                byte[] uncompressed = Zstd.decompress(compressedBloomFilter, sizeBeforeCompressed);
                bloomFilter = BloomFilter.readFrom(uncompressed);
            }
            compressedBloomFilter = null;
        }
    }
}
