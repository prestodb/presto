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
package com.facebook.presto.parquet.crypto.CryptoRetriever;

import com.facebook.presto.parquet.crypto.KeyAccessDeniedException;
import com.facebook.presto.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the InMemory implementation of KMSClient.
 *
 * This is not thread safe.
 */
public class InMemoryKMSClient
        extends KMSClient
{
    public static final String CONF_COLUMN_KEY_LENGTH = "parquet.column.key.length";
    public static final String CONF_TEST_COL_KEY_INVALID = "parquet.test.col.key.invalid";
    public static final String CONF_COL_TEST_KEY_MISSING = "parquet.test.col.key.missing";
    public static final String CONF_TEST_FOOTER_KEY_INVALID = "parquet.test.footer.key.invalid";

    protected Map<ByteBuffer, byte[]> keyMap;
    protected Map<ByteBuffer, byte[]> aadMap;

    private static int keyLength = 16;
    private Configuration conf;

    public InMemoryKMSClient(Configuration conf)
            throws IOException
    {
        super();
        keyMap = new HashMap<>();
        aadMap = new HashMap<>();
        this.conf = conf;
        keyLength = conf.getInt(CONF_COLUMN_KEY_LENGTH, 16);
        generateKeys();
        generateAads();
    }

    /**
     * Get key from store
     * @param keyMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     * @return
     * @throws IOException
     */
    public byte[] getKey(byte[] keyMetadata)
            throws KeyAccessDeniedException, ParquetCryptoRuntimeException
    {
        byte[] key = keyMap.get(ByteBuffer.wrap(keyMetadata));
        if (conf.getBoolean(CONF_TEST_COL_KEY_INVALID, false) && isColumnKey(keyMetadata)) {
            byte[] newKey = key.clone();
            return flipKey(newKey);
        }
        if (conf.getBoolean(CONF_TEST_FOOTER_KEY_INVALID, false) && isFooterKey(keyMetadata)) {
            byte[] newKey = key.clone();
            return flipKey(newKey);
        }
        else if (conf.getBoolean(CONF_COL_TEST_KEY_MISSING, false) && isColumnKey(keyMetadata)) {
            return null;
        }
        else if (conf.getBoolean(CONF_TEST_FOOTER_KEY_INVALID, false) && isFooterKey(keyMetadata)) {
            return null;
        }
        else {
            return key;
        }
    }

    /**
     * Get the AAD
     * @param aadMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     * @return
     */
    public byte[] getAAD(byte[] aadMetadata)
    {
        return aadMap.get(ByteBuffer.wrap(aadMetadata));
    }

    /**
     * Put key to store
     * @param
     *       keyMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     *       key length must be either 16, 24 or 32 bytes.
     * @return
     * @throws IOException
     */
    public void putKey(byte[] keyMetadata, byte[] key)
            throws IOException
    {
        keyMap.put(ByteBuffer.wrap(keyMetadata), key);
    }

    /**
     * Put AAD to store
     * @param
     *       aadMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     *       aad length must be 16 bytes.
     * @return
     * @throws IOException
     */
    public void putAad(byte[] aadMetadata, byte[] aad)
            throws IOException
    {
        aadMap.put(ByteBuffer.wrap(aadMetadata), aad);
    }

    /**
     * Remove key from store
     * @param
     *       keyMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     * @return
     * @throws IOException
     */
    public void removeKey(byte[] keyMetadata)
            throws IOException
    {
        keyMap.remove(keyMetadata);
    }

    /**
     * Remove all keys
     * @throws IOException
     */
    public void removeKeys()
            throws IOException
    {
        keyMap.clear();
    }

    /**
     * Remove AAD from store
     * @param
     *       aadMetadata Parquet-mr doesn't have maximal length restrictions. It depends on user's KMS restrictions.
     * @return
     * @throws IOException
     */
    public void removeAad(byte[] aadMetadata)
            throws IOException
    {
        aadMap.remove(aadMetadata);
    }

    /**
     * Remove AADs from store
     * @return
     * @throws IOException
     */
    public void removeAads()
            throws IOException
    {
        aadMap.clear();
    }

    private void generateKeys()
            throws IOException
    {
        // Generate footer keys. The the key is like this: {keyMetadata: db0.tbl0.footer, keyValue:
        // {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                String footerKeyName = "db" + String.valueOf(i) + ".tbl" + String.valueOf(j) + ".footer";
                putKey(footerKeyName.getBytes(Charset.forName("UTF-8")), shiftKeys(keyLength, i, j, 0));
            }
        }

        // Generate column keys. The the key is like this: {keyMetadata: db0.tbl0.col0, keyValue:
        // {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                for (int k = 0; k < 50; k++) {
                    String colKeyName = "db" + String.valueOf(i) + ".tbl" + String.valueOf(j) + ".col" + String.valueOf(k);
                    putKey(colKeyName.getBytes(Charset.forName("UTF-8")), shiftKeys(keyLength, i, j, k));
                }
            }
        }

        // TODO: This is a temp key we have to keep because Ranger KMS has the same for now.
        putKey("test".getBytes(Charset.forName("UTF-8")), new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 19});
    }

    private byte[] shiftKeys(int keyLengh, int dbNum, int tblNum, int colNum)
    {
        byte[] key;
        if (keyLengh == 16) {
            key = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        }
        else if (keyLengh == 24) {
            key = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        }
        else if (keyLengh == 32) {
            key = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
        }
        else {
            throw new RuntimeException("Invalid key length");
        }

        key[dbNum % keyLengh] = changeKey(key[dbNum % keyLengh]);
        key[tblNum % keyLengh] = changeKey(key[tblNum % keyLengh]);
        key[colNum % keyLengh] = changeKey(key[colNum % keyLengh]);
        return key;
    }

    private byte changeKey(byte key)
    {
        int newKey = (int) (key) + 1;
        return (byte) (newKey & 0xff);
    }

    private void generateAads()
            throws IOException
    {
        putAad(new byte[]{0, 0}, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        putAad(new byte[]{0, 1}, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16});
        putAad(new byte[]{0, 2}, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17});
        putAad(new byte[]{0, 3}, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18});
    }

    public EKeySet generateEK(String mkMetadata)
            throws IOException
    {
        byte[] metdata = mkMetadata.getBytes(Charset.forName("UTF-8"));
        byte[] eekPlaintext = this.keyMap.get(ByteBuffer.wrap(metdata));
        return new KMSClient.EKeySet(eekPlaintext, metdata);
    }

    private byte[] flipKey(byte[] key)
    {
        if (key != null && key.length > 0) {
            key[0] = changeKey(key[0]);
        }
        return key;
    }

    private boolean isFooterKey(byte[] metadata)
    {
        String strMetadata = new String(metadata);
        return strMetadata.endsWith(".footer");
    }

    private boolean isColumnKey(byte[] metadata)
    {
        String strMetadata = new String(metadata);
        return strMetadata.contains(".col");
    }
}
