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

grammar DateFormat;

dateFormat
    : token+ EOF
    ;

token
    : TEXT
    | YYYY
    | YY
    | MM
    ;

DD: 'dd';
HH24: 'hh24';
HH: 'hh';
MM: 'mm';
MI: 'mi';
SS: 'ss';
YYYY: 'yyyy';
YY: 'yy';

TEXT
    : [ \r\n\t]
    | '-'
    | '/'
    | ','
    | '.'
    | ';'
    | ':'
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .+?
    ;