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
package com.facebook.presto.lark.sheets.api;

import com.facebook.presto.lark.sheets.LarkSheetsConfig;
import com.larksuite.oapi.core.AppSettings;
import com.larksuite.oapi.core.AppType;
import com.larksuite.oapi.core.Config;
import com.larksuite.oapi.core.DefaultStore;
import com.larksuite.oapi.core.Domain;
import com.larksuite.oapi.service.drive_permission.v2.DrivePermissionService;
import com.larksuite.oapi.service.sheets.v2.SheetsService;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.function.Supplier;

import static com.facebook.presto.lark.sheets.LarkSheetsUtil.loadAppSecret;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LarkSheetsApiFactory
        implements Supplier<LarkSheetsApi>
{
    private final DrivePermissionService drivePermissionService;
    private final SheetsService sheetsService;

    @Inject
    public LarkSheetsApiFactory(LarkSheetsConfig config)
    {
        this(requireNonNull(config, "config is null").getAppDomain(), config.getAppId(), config.getAppSecretFile());
    }

    public LarkSheetsApiFactory(LarkSheetsConfig.Domain domain, String appId, String appSecretFile)
    {
        requireNonNull(domain, "domain is null");
        requireNonNull(appId, "appId is null");
        requireNonNull(appSecretFile, "appSecretFile is null");

        String appSecret = loadAppSecret(appSecretFile);
        AppSettings appSettings = new AppSettings(AppType.Internal, appId, appSecret, null, null);
        Config config = new Config(toLarkDomain(domain), appSettings, new DefaultStore());
        drivePermissionService = new DrivePermissionService(config);
        sheetsService = new SheetsService(config);
    }

    @Override
    public LarkSheetsApi get()
    {
        return new SimpleLarkSheetsApi(drivePermissionService, sheetsService);
    }

    private static Domain toLarkDomain(LarkSheetsConfig.Domain domain)
    {
        switch (domain) {
            case LARK:
                return Domain.LarkSuite;
            case FEISHU:
                return Domain.FeiShu;
            default:
                throw new IllegalArgumentException("Invalid domain " + domain);
        }
    }
}
