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
/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */

#ifndef DS_ADDRESS_H
#define DS_ADDRESS_H

#include "constants.h"
#include "dist.h"

#define DS_ADDR_SUITE_NUM 0
#define DS_ADDR_STREET_NUM 1
#define DS_ADDR_STREET_NAME1 2
#define DS_ADDR_STREET_NAME2 3
#define DS_ADDR_STREET_TYPE 4
#define DS_ADDR_CITY 5
#define DS_ADDR_COUNTY 6
#define DS_ADDR_STATE 7
#define DS_ADDR_COUNTRY 8
#define DS_ADDR_ZIP 9
#define DS_ADDR_PLUS4 10
#define DS_ADDR_GMT_OFFSET 11

int mk_address(ds_addr_t* pDest, int nColumn, DSDGenContext& dsdGenContext);
int mk_streetnumber(int nTable, int* dest, DSDGenContext& dsdGenContext);
int mk_suitenumber(int nTable, char* dest, DSDGenContext& dsdGenContext);
int mk_streetname(int nTable, char* dest);
int mk_city(int nTable, char** dest, DSDGenContext& dsdGenContext);
int city_hash(int nTable, char* name);
int mk_zipcode(
    int nTable,
    char* dest,
    int nRegion,
    char* city,
    DSDGenContext& dsdGenContext);
// void printAddressPart(FILE *fp, ds_addr_t *pAddr, int nAddressPart);
void resetCountCount(void);

#endif
