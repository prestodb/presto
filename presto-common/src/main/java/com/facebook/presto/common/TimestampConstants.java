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
package com.facebook.presto.common;

public final class TimestampConstants
{
    private static final int MAX_PICOS_OF_MICRO = 999_999;

    private TimestampConstants() {}

    public static void checkPicosOfMicro(int picosOfMicro)
    {
        if (picosOfMicro < 0 || picosOfMicro > MAX_PICOS_OF_MICRO) {
            throw new IllegalArgumentException(
                    "picosOfMicro must be in [0, " + MAX_PICOS_OF_MICRO + "]: " + picosOfMicro);
        }
    }
}
