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
package com.facebook.presto.memory;

import static com.google.common.base.Preconditions.checkArgument;

public class Reservations
{
    private Reservations() {}

    public static void checkReservedBytes(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
    }

    public static void checkFreedBytes(long bytes, long currentReservation)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= currentReservation, "tried to free more than is reserved");
    }
}
