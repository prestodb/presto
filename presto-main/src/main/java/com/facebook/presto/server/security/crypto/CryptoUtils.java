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
package com.facebook.presto.server.security.crypto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.security.CryptoLibrary;
import com.google.common.collect.Maps;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CryptoUtils
{
    private static final Logger log = Logger.get(CryptoUtils.class);

    private CryptoUtils()
    {}

    public static Map<String, String> decryptProperties(Map<String, String> encProps)
    {
        Map<String, String> props = new HashMap<>();
        Set<Map.Entry<String, String>> entrySet = encProps.entrySet();
        for (Map.Entry<String, String> entry :
                entrySet) {
            String key = entry.getKey();
            String encStr = entry.getValue();
            final int outPlaintextLength = 2 * 4096;
            byte[] outPlaintext = new byte[outPlaintextLength];
            int decResp = CryptoLibrary.INSTANCE.do_decrypt_string(encStr, outPlaintext, outPlaintextLength);
            props.put(key, new String(outPlaintext, 0, decResp, UTF_8));
        }
        return props;
    }

    public static Map<String, String> loadDecryptedProperties(String fileLocation)
    {
        if (!isNullOrEmpty(fileLocation)) {
            try (InputStream inputStream = Files.newInputStream(Paths.get(fileLocation))) {
                Properties props = new Properties();
                props.load(inputStream);
                return decryptProperties(new HashMap<>(Maps.fromProperties(props)));
            }
            catch (Exception exc) {
                log.error(exc, "Unable to load decrypted properties from file path '%s'", fileLocation);
            }
        }
        return Collections.emptyMap();
    }
}
