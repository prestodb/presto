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
package com.facebook.presto.spi.error;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import static java.util.Objects.requireNonNull;

public class ErrorRetriever
{
    private static boolean isErrorI18nEnabled;
    private static List<Locale> availableBundles = Collections.emptyList();

    private ErrorRetriever() {}

    public static void preloadErrorBundles(boolean isErrorI18nEnabled)
    {
        ErrorRetriever.isErrorI18nEnabled = isErrorI18nEnabled;
        ResourceBundle.getBundle("Messages");
        if (isErrorI18nEnabled) {
            // If internationalization is enabled, load the locale specific error bundles
            // already available.
            availableBundles.forEach(locale -> ResourceBundle.getBundle("Messages", locale));
        }
    }

    public static String getErrorMessage(String errorKey, Locale locale)
    {
        if (isErrorI18nEnabled) {
            requireNonNull(locale, "locale cannot be null");
        }
        else {
            locale = Locale.US;
        }

        // ResourceBundle.getBundle returns cached instances of the bundle
        ResourceBundle selectedBundle = ResourceBundle.getBundle("Messages", locale);
        return selectedBundle.getString(errorKey);
    }
}
