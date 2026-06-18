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

import com.facebook.presto.lark.sheets.LarkSheetsErrorCode;
import com.facebook.presto.lark.sheets.LarkSheetsUtil;
import com.facebook.presto.spi.PrestoException;
import com.larksuite.oapi.core.api.response.Response;
import com.larksuite.oapi.service.drive_permission.v2.DrivePermissionService;
import com.larksuite.oapi.service.drive_permission.v2.model.PublicGetReqBody;
import com.larksuite.oapi.service.drive_permission.v2.model.PublicGetResult;
import com.larksuite.oapi.service.sheets.v2.SheetsService;
import com.larksuite.oapi.service.sheets.v2.model.MetainfoProperties;
import com.larksuite.oapi.service.sheets.v2.model.Sheet;
import com.larksuite.oapi.service.sheets.v2.model.SpreadsheetsMetainfoResult;
import com.larksuite.oapi.service.sheets.v2.model.SpreadsheetsValuesGetResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.LARK_API_ERROR;
import static com.facebook.presto.lark.sheets.LarkSheetsUtil.mask;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SimpleLarkSheetsApi
        implements LarkSheetsApi
{
    private final DrivePermissionService drivePermissionService;
    private final SheetsService sheetsService;

    public SimpleLarkSheetsApi(DrivePermissionService drivePermissionService, SheetsService sheetsService)
    {
        this.drivePermissionService = requireNonNull(drivePermissionService, "drivePermissionService is null");
        this.sheetsService = requireNonNull(sheetsService, "sheetsService is null");
    }

    @Override
    public boolean isReadable(String token)
    {
        try {
            PublicGetReqBody request = new PublicGetReqBody();
            request.setType("sheet");
            request.setToken(token);

            Response<PublicGetResult> response = drivePermissionService.getPublics().get(request).execute();
            // sheet is readable only when it is public shared
            return response.getCode() == 0;
        }
        catch (Exception e) {
            throw wrapApiError(e, "Could not check permission for spreadsheet " + mask(token));
        }
    }

    @Override
    public SpreadsheetInfo getMetaInfo(String token)
    {
        SheetsService.SpreadsheetsMetainfoReqCall request = sheetsService.getSpreadsheetss().metainfo();
        request.setSpreadsheetToken(token);
        try {
            Response<SpreadsheetsMetainfoResult> response = request.execute();
            checkResponse(response);
            SpreadsheetsMetainfoResult data = response.getData();
            MetainfoProperties properties = data.getProperties();
            List<SheetInfo> sheets = Arrays.stream(data.getSheets())
                    .map(sheet -> toMetaInfoSheet(token, sheet))
                    .collect(toImmutableList());
            return new SpreadsheetInfo(data.getSpreadsheetToken(), properties.getTitle(), properties.getRevision(), sheets);
        }
        catch (Exception e) {
            throw wrapApiError(e, "Could not get metadata of spreadsheet " + mask(token));
        }
    }

    @Override
    public List<String> getHeaderRow(String token, String sheetId, int columnCount)
    {
        String lastColumnLabel = LarkSheetsUtil.columnIndexToColumnLabel(columnCount - 1);
        String range = format("%s!A1:%s1", sheetId, lastColumnLabel);
        try {
            SheetsService.SpreadsheetsValuesGetReqCall request = sheetsService.getSpreadsheetss().valuesGet();
            request.setSpreadsheetToken(token);
            request.setRange(range);

            Response<SpreadsheetsValuesGetResult> response = request.execute();
            checkResponse(response);
            SpreadsheetsValuesGetResult data = response.getData();
            Object[] values = data.getValueRange().getValues();
            if (values.length == 0) {
                throw new PrestoException(LarkSheetsErrorCode.SHEET_BAD_DATA,
                        format("Sheet %s.%s is empty", mask(token), sheetId));
            }
            if (!(values[0] instanceof List)) {
                throw new PrestoException(LARK_API_ERROR,
                        format("Illegal response data of sheet %s @ %s", mask(token), range));
            }
            List<?> header = ((List<?>) values[0]);
            return header.stream().map(obj -> obj == null ? null : obj.toString()).collect(Collectors.toList());
        }
        catch (Exception e) {
            throw wrapApiError(e, format("Could not get data of sheet %s @ %s", mask(token), range));
        }
    }

    @Override
    public SheetValues getValues(String token, String range)
    {
        try {
            SheetsService.SpreadsheetsValuesGetReqCall request = sheetsService.getSpreadsheetss().valuesGet();
            request.setSpreadsheetToken(token);
            request.setRange(range);

            Response<SpreadsheetsValuesGetResult> response = request.execute();
            checkResponse(response);
            SpreadsheetsValuesGetResult data = response.getData();

            // Convert Object[] to List<List<Object>>
            Object[] arr = data.getValueRange().getValues();
            List<List<Object>> values = new ArrayList<>(arr.length);
            for (Object item : arr) {
                if (item instanceof List) {
                    values.add(((List) item));
                }
                else {
                    throw new PrestoException(LARK_API_ERROR,
                            format("Illegal response data of sheet %s @ %s", mask(token), range));
                }
            }

            return new SheetValues(
                    data.getRevision(),
                    data.getValueRange().getRange(),
                    values);
        }
        catch (Exception e) {
            throw wrapApiError(e, format("Could not get data of sheet %s @ %s", mask(token), range));
        }
    }

    private static void checkResponse(Response<?> response)
    {
        if (response.getCode() != 0) {
            throw new PrestoException(LARK_API_ERROR,
                    format("Bad response: [%d] %s", response.getCode(), response.getMsg()));
        }
    }

    private static PrestoException wrapApiError(Exception e, String message)
    {
        if (e instanceof PrestoException) {
            throw (PrestoException) e;
        }
        throw new PrestoException(LARK_API_ERROR, message, e);
    }

    private static SheetInfo toMetaInfoSheet(String token, Sheet sheet)
    {
        return new SheetInfo(
                token,
                sheet.getSheetId(),
                sheet.getTitle(),
                sheet.getIndex(),
                sheet.getColumnCount(),
                sheet.getRowCount());
    }
}
