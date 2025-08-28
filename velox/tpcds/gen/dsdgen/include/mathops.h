/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#define OP_PLUS 0x00001
#define OP_MINUS 0x00002
#define OP_MULT 0x00004
#define OP_DIV 0x00008
#define OP_MOD 0x00010
#define OP_XOR 0x00020
#define OP_PAREN 0x00040
#define OP_BRACKET 0x00080
#define OP_NEST 0x00100 /* a --> (a) */
#define OP_NEG 0x00200
#define OP_ADDR 0x00400 /* get an address */
#define OP_PTR 0x00800 /* reference through a pointer */
#define OP_FUNC 0x01000 /* user function/macro */
#define OP_UNIQUE 0x02000 /* built in functions start here */
#define OP_TEXT 0x04000
#define OP_RANDOM 0x08000
#define OP_RANGE 0x10000
#define OP_USER 0x20000 /* user defined function */
