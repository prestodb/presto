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
package com.facebook.presto.iceberg.nessie;

public enum AuthenticationType
{
    /**
     * Nessie BASIC authentication type is deprecated, this auth type will be removed in upcoming release.
     * https://github.com/projectnessie/nessie/blob/nessie-0.59.0/api/client/src/main/java/org/projectnessie/client/NessieConfigConstants.java#L34
     */
    @Deprecated
    BASIC,
    BEARER
}
