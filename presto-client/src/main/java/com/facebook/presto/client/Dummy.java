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
package com.facebook.presto.client;
import okio.ByteString;
/**
 * This Dummy class ensures Okio is included in the build to address dependency conflicts between OkHttp and Okio regarding Kotlin libraries.
 * It prevents the Maven enforcer plugin from incorrectly flagging Okio as unused, maintaining necessary dependencies without runtime issues.
 */
public class Dummy
{
    static {
        okio.ByteString byteString = new ByteString(new byte[] {});
        System.out.println(byteString);
    }
}
