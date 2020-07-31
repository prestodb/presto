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
package com.facebook.presto.hive;

public class EncryptionProperties
{
    public static final String ENCRYPT_COLUMNS_KEY = "encrypt.columns";
    public static final String ENCRYPT_TABLE_KEY = "encrypt.table";
    public static final String DWRF_ENCRYPTION_ALGORITHM_KEY = "dwrf.encryption.algorithm";
    public static final String DWRF_ENCRYPTION_PROVIDER_KEY = "dwrf.encryption.provider";

    private EncryptionProperties()
    {
    }
}
