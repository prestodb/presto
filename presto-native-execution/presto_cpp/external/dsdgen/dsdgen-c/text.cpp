/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include "config.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "genrand.h"
#include "porting.h"

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
  char* verbiage = NULL;
  int used_space = 0; /* current length of the sentence being built */
  int allocated_space = 0;
  int word_len;
  char *syntax, *cp, *word = NULL, temp[2];

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

    if (word == NULL)
      word_len = 1;
    else
      word_len = strlen(word);

    if (used_space + word_len >= allocated_space) {
      verbiage = (char*)realloc(verbiage, allocated_space + SPACE_INCREMENT);
      MALLOC_CHECK(verbiage);
      allocated_space += SPACE_INCREMENT;
    }

    if (word == NULL)
      strcpy(&verbiage[used_space], temp);
    else
      strcpy(&verbiage[used_space], word);
    used_space += word_len;
    word = NULL;
  }

  return (verbiage);
}

/*
 * Routine: gen_text()
 * Purpose: entry point for this module. Generate a truncated sentence in a
 *			given length range
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
char* gen_text(
    char* dest,
    int min,
    int max,
    int stream,
    DSDGenContext& dsdGenContext) {
  int target_len, generated_length, capitalize = 1;
  char* s;

  genrand_integer(
      &target_len, DIST_UNIFORM, min, max, 0, stream, dsdGenContext);
  if (dest)
    *dest = '\0';
  else {
    dest = (char*)malloc((max + 1) * sizeof(char));
    MALLOC_CHECK(dest);
  }

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
  }

  return (dest);
}

#ifdef TEST
#define DECLARER
#include "r_driver.h"
#include "r_params.h"

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
