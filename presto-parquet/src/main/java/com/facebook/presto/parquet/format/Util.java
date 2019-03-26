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
package com.facebook.presto.parquet.format;

import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.InterningProtocol;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.event.Consumers.Consumer;
import org.apache.parquet.format.event.Consumers.DelegatingFieldConsumer;
import org.apache.parquet.format.event.EventBasedThriftReader;
import org.apache.parquet.format.event.TypedConsumer.I32Consumer;
import org.apache.parquet.format.event.TypedConsumer.I64Consumer;
import org.apache.parquet.format.event.TypedConsumer.StringConsumer;
import shaded.parquet.org.apache.thrift.TBase;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.protocol.TProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.parquet.format.FileMetaData._Fields.CREATED_BY;
import static org.apache.parquet.format.FileMetaData._Fields.ENCRYPTION_ALGORITHM;
import static org.apache.parquet.format.FileMetaData._Fields.FOOTER_SIGNING_KEY_METADATA;
import static org.apache.parquet.format.FileMetaData._Fields.KEY_VALUE_METADATA;
import static org.apache.parquet.format.FileMetaData._Fields.NUM_ROWS;
import static org.apache.parquet.format.FileMetaData._Fields.ROW_GROUPS;
import static org.apache.parquet.format.FileMetaData._Fields.SCHEMA;
import static org.apache.parquet.format.FileMetaData._Fields.VERSION;
import static org.apache.parquet.format.event.Consumers.fieldConsumer;
import static org.apache.parquet.format.event.Consumers.listElementsOf;
import static org.apache.parquet.format.event.Consumers.listOf;
import static org.apache.parquet.format.event.Consumers.struct;

/**
 * Utility to read/write metadata
 * We use the TCompactProtocol to serialize metadata
 */
public class Util
{
    private Util()
    {
    }

    public static PageHeader readPageHeader(InputStream from)
            throws IOException
    {
        return readPageHeader(from, null, null);
    }

    public static PageHeader readPageHeader(InputStream from, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        return read(from, new PageHeader(), decryptor, aad);
    }

    public static FileMetaData readFileMetaData(InputStream from)
            throws IOException
    {
        return readFileMetaData(from, null, null);
    }

    public static FileMetaData readFileMetaData(InputStream from, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        return read(from, new FileMetaData(), decryptor, aad);
    }

    public static ColumnMetaData readColumnMetaData(InputStream from, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        return read(from, new ColumnMetaData(), decryptor, aad);
    }

    /**
     * reads the meta data from the stream
     * @param from the stream to read the metadata from
     * @param skipRowGroups whether row groups should be skipped
     * @return the resulting metadata
     * @throws IOException if any I/O error occurs during the reading
     */
    public static FileMetaData readFileMetaData(InputStream from, boolean skipRowGroups)
            throws IOException
    {
        return readFileMetaData(from, skipRowGroups, (BlockCipher.Decryptor) null, (byte[]) null);
    }

    public static FileMetaData readFileMetaData(InputStream from, boolean skipRowGroups, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        FileMetaData md = new FileMetaData();
        if (skipRowGroups) {
            readFileMetaData(from, new DefaultFileMetaDataConsumer(md), skipRowGroups, decryptor, aad);
        }
        else {
            read(from, md, decryptor, aad);
        }
        return md;
    }

    public static FileCryptoMetaData readFileCryptoMetaData(InputStream from)
            throws IOException
    {
        return read(from, new FileCryptoMetaData(), null, null);
    }

    public static void readFileMetaData(InputStream from, FileMetaDataConsumer consumer)
            throws IOException
    {
        readFileMetaData(from, consumer, null, null);
    }

    public static void readFileMetaData(InputStream from, FileMetaDataConsumer consumer,
                                        BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        readFileMetaData(from, consumer, false, decryptor, aad);
    }

    public static void readFileMetaData(InputStream from, final FileMetaDataConsumer consumer, boolean skipRowGroups)
            throws IOException
    {
        readFileMetaData(from, consumer, skipRowGroups, null, null);
    }

    public static void readFileMetaData(final InputStream input, final FileMetaDataConsumer consumer,
                                        boolean skipRowGroups, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        try {
            DelegatingFieldConsumer eventConsumer = fieldConsumer()
                    .onField(VERSION, new I32Consumer()
                    {
                        @Override
                        public void consume(int value)
                        {
                            consumer.setVersion(value);
                        }
                    }).onField(SCHEMA, listOf(SchemaElement.class, new Consumer<List<SchemaElement>>()
                    {
                        @Override
                        public void consume(List<SchemaElement> schema)
                        {
                            consumer.setSchema(schema);
                        }
                    })).onField(NUM_ROWS, new I64Consumer()
                    {
                        @Override
                        public void consume(long value)
                        {
                            consumer.setNumRows(value);
                        }
                    }).onField(KEY_VALUE_METADATA, listElementsOf(struct(KeyValue.class, new Consumer<KeyValue>()
                    {
                        @Override
                        public void consume(KeyValue kv)
                        {
                            consumer.addKeyValueMetaData(kv);
                        }
                    }))).onField(CREATED_BY, new StringConsumer()
                    {
                        @Override
                        public void consume(String value)
                        {
                            consumer.setCreatedBy(value);
                        }
                    }).onField(ENCRYPTION_ALGORITHM, struct(EncryptionAlgorithm.class, new Consumer<EncryptionAlgorithm>()
                    {
                        @Override
                        public void consume(EncryptionAlgorithm encryptionAlgorithm)
                        {
                            consumer.setEncryptionAlgorithm(encryptionAlgorithm);
                        }
                    })).onField(FOOTER_SIGNING_KEY_METADATA, new StringConsumer()
                    {
                        @Override
                        public void consume(String value)
                        {
                            byte[] keyMetadata = value.getBytes(StandardCharsets.UTF_8);
                            consumer.setFooterSigningKeyMetadata(keyMetadata);
                        }
                    });

            if (!skipRowGroups) {
                eventConsumer = eventConsumer.onField(ROW_GROUPS, listElementsOf(struct(RowGroup.class, new Consumer<RowGroup>()
                {
                    @Override
                    public void consume(RowGroup rowGroup)
                    {
                        consumer.addRowGroup(rowGroup);
                    }
                })));
            }

            final InputStream from;
            if (null == decryptor) {
                from = input;
            }
            else {
                byte[] plainText = decryptor.decrypt(input, aad);
                from = new ByteArrayInputStream(plainText);
            }
            new EventBasedThriftReader(protocol(from)).readStruct(eventConsumer);
        }
        catch (TException e) {
            throw new IOException("can not read FileMetaData: " + e.getMessage(), e);
        }
    }

    /**
     * To read metadata in a streaming fashion.
     *
     */
    public abstract static class FileMetaDataConsumer
    {
        public abstract void setVersion(int version);
        public abstract void setSchema(List<SchemaElement> schema);
        public abstract void setNumRows(long numRows);
        public abstract void addRowGroup(RowGroup rowGroup);
        public abstract void addKeyValueMetaData(KeyValue kv);
        public abstract void setCreatedBy(String createdBy);
        public abstract void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm);
        public abstract void setFooterSigningKeyMetadata(byte[] footerSigningKeyMetadata);
    }

    /**
     * Simple default consumer that sets the fields
     *
     */
    public static final class DefaultFileMetaDataConsumer
            extends FileMetaDataConsumer
    {
        private final FileMetaData md;

        public DefaultFileMetaDataConsumer(FileMetaData md)
        {
            this.md = md;
        }

        @Override
        public void setVersion(int version)
        {
            md.setVersion(version);
        }

        @Override
        public void setSchema(List<SchemaElement> schema)
        {
            md.setSchema(schema);
        }

        @Override
        public void setNumRows(long numRows)
        {
            md.setNum_rows(numRows);
        }

        @Override
        public void setCreatedBy(String createdBy)
        {
            md.setCreated_by(createdBy);
        }

        @Override
        public void addRowGroup(RowGroup rowGroup)
        {
            md.addToRow_groups(rowGroup);
        }

        @Override
        public void addKeyValueMetaData(KeyValue kv)
        {
            md.addToKey_value_metadata(kv);
        }

        @Override
        public void setEncryptionAlgorithm(EncryptionAlgorithm encryptionAlgorithm)
        {
            md.setEncryption_algorithm(encryptionAlgorithm);
        }

        @Override
        public void setFooterSigningKeyMetadata(byte[] footerSigningKeyMetadata)
        {
            md.setFooter_signing_key_metadata(footerSigningKeyMetadata);
        }
    }

    private static TProtocol protocol(InputStream from)
    {
        return protocol(new TIOStreamTransport(from));
    }

    private static InterningProtocol protocol(TIOStreamTransport t)
    {
        return new InterningProtocol(new TCompactProtocol(t));
    }

    private static <T extends TBase<?, ?>> T read(final InputStream input, T tbase, BlockCipher.Decryptor decryptor, byte[] aad)
            throws IOException
    {
        final InputStream from;
        if (null == decryptor) {
            from = input;
        }
        else {
            byte[] plainText = decryptor.decrypt(input, aad);
            from = new ByteArrayInputStream(plainText);
        }

        try {
            tbase.read(protocol(from));
            return tbase;
        }
        catch (TException e) {
            throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
        }
    }
}
