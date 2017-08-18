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
package com.facebook.presto.execution.buffer;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

public class TestEncryption
{
    @Test
    public void testAES256Encryptor() throws Exception
    {
        Encryptor encryptor = new AES256Encryptor();
        String input1 = "Test input \n1234567890;:'\" `~!@#$%^&*()_+-={}[],.<>/?\\";
        String input2 = "asdfAasdf;LJasdfj;908q209812-09348710293847-09ajsdf98hB";

        byte[] inputData1 = input1.getBytes();
        byte[] encrypted1 = encryptor.encrypt(inputData1);
        assertNotSame(inputData1, encrypted1);

        byte[] inputData2 = input2.getBytes();
        byte[] encrypted2 = encryptor.encrypt(inputData2);
        assertNotSame(inputData2, encrypted2);

        byte[] decryptedData1 = encryptor.decrypt(encrypted1);
        assertEquals(inputData1, decryptedData1);

        byte[] decryptedData2 = encryptor.decrypt(encrypted2);
        assertEquals(inputData2, decryptedData2);
    }
}
