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
package com.facebook.presto.common.security;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface CryptoLibrary
        extends Library
{
    CryptoLibrary INSTANCE = (CryptoLibrary)
            Native.load("ibmlhpreloader", CryptoLibrary.class);

    int do_encrypt_string(String inPlaintext, byte[] outCiphertextB64, int outCiphertextB64Length);

    int do_decrypt_string(String inCiphertextB64, byte[] outPlaintext, int outPlaintextLength);
}
