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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"

/*
 * Routine: mk_sentence()
 * Purpose: create a sample sentence
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
#define SPACE_INCREMENT 100

char* mk_sentence(int stream, DSDGenContext& dsdGenContext) {
  char* verbiage = nullptr;
  int used_space = 0; /* current length of the sentence being built */
  int allocated_space = 0;
  int word_len = 0;
  char *syntax = nullptr, *cp = nullptr, *word = nullptr, temp[2];

  temp[1] = '\0';
  pick_distribution(&syntax, "sentences", 1, 1, stream, dsdGenContext);

  for (cp = syntax; *cp; cp++) {
    switch (*cp) {
      case 'N': /* pick a noun */
        pick_distribution(&word, "nouns", 1, 1, stream, dsdGenContext);
        break;
      case 'V': /* pick a verb */
        pick_distribution(&word, "verbs", 1, 1, stream, dsdGenContext);
        break;
      case 'J': /* pick a adjective */
        pick_distribution(&word, "adjectives", 1, 1, stream, dsdGenContext);
        break;
      case 'D': /* pick a adverb */
        pick_distribution(&word, "adverbs", 1, 1, stream, dsdGenContext);
        break;
      case 'X': /* pick a auxiliary verb */
        pick_distribution(&word, "auxiliaries", 1, 1, stream, dsdGenContext);
        break;
      case 'P': /* pick a preposition */
        pick_distribution(&word, "prepositions", 1, 1, stream, dsdGenContext);
        break;
      case 'A': /* pick an article */
        pick_distribution(&word, "articles", 1, 1, stream, dsdGenContext);
        break;
      case 'T': /* pick an terminator */
        pick_distribution(&word, "terminators", 1, 1, stream, dsdGenContext);
        break;
      default:
        temp[0] = *cp;
        break;
    }

    if (word == nullptr)
      word_len = 1;
    else
      word_len = strlen(word);

    if (used_space + word_len >= allocated_space) {
      verbiage = static_cast<char*>(
          realloc(verbiage, allocated_space + SPACE_INCREMENT));
      MALLOC_CHECK(verbiage);
      allocated_space += SPACE_INCREMENT;
    }

    if (word == nullptr)
      strcpy(&verbiage[used_space], temp);
    else
      strcpy(&verbiage[used_space], word);
    used_space += word_len;
    word = nullptr;
  }

  return (verbiage);
}

/*
 * Routine: gen_text()
 * Purpose: entry point for this module. Generate a truncated sentence in a
 *                     given length range
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void gen_text(
    char* dest,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext) {
  int target_len = 0, generated_length = 0, capitalize = 1;
  char* s = nullptr;

  genrand_integer(
      &target_len, DIST_UNIFORM, min, max, 0, stream, dsdGenContext);
  if (dest)
    *dest = '\0';

  while (target_len > 0) {
    s = mk_sentence(stream, dsdGenContext);
    if (capitalize)
      *s = toupper(*s);
    generated_length = strlen(s);
    capitalize = (s[generated_length - 1] == '.');
    if (target_len <= generated_length)
      s[target_len] = '\0';
    strcat(dest, s);
    target_len -= generated_length;
    if (target_len > 0) {
      strcat(dest, " ");
      target_len -= 1;
    }
    free(s);
  }
}

#ifdef TEST
#define DECLARER
#include "velox/tpcds/gen/dsdgen/include/r_driver.h" // @manual
#include "velox/tpcds/gen/dsdgen/include/r_params.h" // @manual

typedef struct {
  char* name;
} tdef;
/* tdef tdefs[] = {NULL}; */

option_t options[] = {

    {"DISTRIBUTIONS", OPT_STR, 0, NULL, NULL, "tester_dist.idx"},
    NULL};

char* params[2];

main() {
  char test_dest[201];
  int i;

  init_params();

  for (i = 0; i < 100; i++) {
    gen_text(test_dest, 100, 200, 1);
    printf("%s\n", test_dest);
    test_dest[0] = '\0';
  }

  return (0);
}
#endif
