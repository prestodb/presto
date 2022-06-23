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
package com.facebook.presto.lark.sheets;

import com.facebook.airlift.configuration.Config;

public class LarkSheetsConfig
{
    public enum Domain
    {
        // For feishu.cn users
        FEISHU,
        // For larksuite.com users
        LARK;
    }

    private Domain appDomain = Domain.LARK;
    private String appId;
    private String appSecretFile;

    public Domain getAppDomain()
    {
        return appDomain;
    }

    @Config("app-domain")
    public LarkSheetsConfig setAppDomain(Domain appDomain)
    {
        this.appDomain = appDomain;
        return this;
    }

    public String getAppId()
    {
        return appId;
    }

    @Config("app-id")
    public LarkSheetsConfig setAppId(String appId)
    {
        this.appId = appId;
        return this;
    }

    public String getAppSecretFile()
    {
        return appSecretFile;
    }

    @Config("app-secret-file")
    public LarkSheetsConfig setAppSecretFile(String appSecretFile)
    {
        this.appSecretFile = appSecretFile;
        return this;
    }
}
