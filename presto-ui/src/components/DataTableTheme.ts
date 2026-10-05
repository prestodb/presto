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
import { createTheme, Theme } from "react-data-table-component";

// react-data-table-component 8.x no longer ships a built-in "dark" theme (dark appearance is
// driven by `colorMode` instead), so `theme="dark"` silently falls back to the light theme.
// Recreate the 7.x dark palette here, with the transparent background Presto always used on top
// of it, so the tables keep rendering as they did before the upgrade.
export const PRESTO_DARK_THEME: Theme = createTheme({
    colorScheme: "dark",
    // 8.x draws column separators in the header by default; 7.x did not.
    headerSeparator: false,
    columnSeparator: false,
    text: {
        primary: "#FFFFFF",
        secondary: "rgba(255, 255, 255, 0.7)",
        disabled: "rgba(0,0,0,.12)",
    },
    background: {
        default: "transparent",
    },
    context: {
        background: "#E91E63",
        text: "#FFFFFF",
    },
    divider: {
        default: "rgba(81, 81, 81, 1)",
    },
    button: {
        default: "#FFFFFF",
        focus: "rgba(255, 255, 255, .54)",
        hover: "rgba(255, 255, 255, .12)",
        disabled: "rgba(255, 255, 255, .18)",
    },
    selected: {
        default: "rgba(0, 0, 0, .7)",
        text: "#FFFFFF",
    },
    highlightOnHover: {
        default: "rgba(0, 0, 0, .7)",
        text: "#FFFFFF",
    },
    striped: {
        default: "rgba(0, 0, 0, .87)",
        text: "#FFFFFF",
    },
});
