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
package com.facebook.presto;

import com.facebook.presto.spi.RequestModifier;

import java.util.ArrayList;
import java.util.List;

public class RequestModifierManager
{
    private final List<RequestModifier> requestModifiers;
    public RequestModifierManager()
    {
        this.requestModifiers = new ArrayList<>();
    }

    public List<RequestModifier> getRequestModifiers()
    {
        return new ArrayList<>(requestModifiers);
    }

    public void registerRequestModifier(RequestModifier requestModifier)
    {
        requestModifiers.add(requestModifier);
    }
}
