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

public enum SseType
{
    KMS("KMS"),
    S3("S3");

    private final String algorithm;

    public String getAlgorithm()
    {
        return algorithm;
    }

    private SseType(String algorithm)
    {
        this.algorithm = algorithm;
    }

    @Override
    public String toString()
    {
        return algorithm;
    }

    public static SseType getValueOf(String algorithm)
    {
        if (algorithm == null) {
            return S3;
        }
        for (SseType sse : values()) {
            if (sse.getAlgorithm().equalsIgnoreCase(algorithm)) {
                return sse;
            }
        }
        throw new IllegalArgumentException();
    }
}
