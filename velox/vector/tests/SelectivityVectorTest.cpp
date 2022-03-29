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

#include "velox/vector/SelectivityVector.h"

#include <gtest/gtest.h>

namespace facebook {
namespace velox {
namespace test {
namespace {

void assertState(
    const std::vector<bool>& expected,
    const SelectivityVector& selectivityVector,
    bool testProperties = true) {
  ASSERT_EQ(expected.size(), selectivityVector.size());

  size_t startIndexIncl = 0;
  size_t endIndexExcl = 0;

  size_t count = 0;

  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(expected.at(i), selectivityVector.isValid(i))
        << "Mismatch at index " << i
        << ", selectivityVector: " << selectivityVector
        << ", expected: " << ::testing::PrintToString(expected);
    if (expected.at(i)) {
      endIndexExcl = i + 1;
      if (count == 0) {
        startIndexIncl = i;
      }

      ++count;
    }
  }

  if (testProperties) {
    ASSERT_EQ(startIndexIncl, selectivityVector.begin()) << selectivityVector;
    ASSERT_EQ(endIndexExcl, selectivityVector.end()) << selectivityVector;
    ASSERT_EQ(startIndexIncl < endIndexExcl, selectivityVector.hasSelections())
        << selectivityVector;
    // countSelected relies on startIndexIncl and endIndexExcl being correct
    // hence why it's inside this if block.
    ASSERT_EQ(count, selectivityVector.countSelected());
  }
}

void assertIsValid(
    int from,
    int to,
    const SelectivityVector& vector,
    bool value) {
  for (int i = 0; i < from; i++) {
    EXPECT_EQ(!value, vector.isValid(i)) << "at " << i;
  }
  for (int i = from; i < to; i++) {
    EXPECT_EQ(value, vector.isValid(i)) << "at " << i;
  }
}

void setValid_normal(bool setToValue) {
  // A little bit more than 2 simd widths, so overflow.
  const size_t vectorSize = 513;

  // We'll set everything to the opposite of the setToValue
  std::vector<bool> expected(vectorSize, !setToValue);
  SelectivityVector vector(vectorSize);
  if (setToValue) {
    vector.clearAll();
  }

  auto setAndAssert = [&](size_t index, bool value) {
    vector.setValid(index, value);
    expected[index] = value;
    ASSERT_NO_FATAL_FAILURE(
        assertState(expected, vector, false /*testProperties*/))
        << "Setting to " << value << " at " << index;
  };

  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_NO_FATAL_FAILURE(setAndAssert(i, setToValue));
    ASSERT_NO_FATAL_FAILURE(setAndAssert(i, !setToValue));
  }
}

}; // namespace

TEST(SelectivityVectorTest, setValid_true_normal) {
  setValid_normal(true);
}

TEST(SelectivityVectorTest, setValid_false_normal) {
  setValid_normal(false);
}

TEST(SelectivityVectorTest, basicOperation) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "Starting state";

  vector.clearAll();
  expected = std::vector<bool>(vectorSize, false);
  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "clearAll() called";

  vector.setAll();
  expected = std::vector<bool>(vectorSize, true);
  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector)) << "setAll() called";
}

TEST(SelectivityVectorTest, updateBounds) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  auto setAndAssert = [&](size_t index, bool value) {
    vector.setValid(index, value);
    vector.updateBounds();

    expected[index] = value;

    assertState(expected, vector);
  };

  ASSERT_NO_FATAL_FAILURE(setAndAssert(9, false));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(0, false));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(9, true));
  ASSERT_NO_FATAL_FAILURE(setAndAssert(0, true));
}

TEST(SelectivityVectorTest, merge) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  other.setValid(5, false);
  other.updateBounds();
  vector.intersect(other);

  std::vector<bool> expected(vectorSize, true);
  expected[5] = false;

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, deselect) {
  const size_t vectorSize = 10;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  other.clearAll();
  other.setValid(5, true);
  other.updateBounds();
  vector.deselect(other);

  std::vector<bool> expected(vectorSize, true);
  expected[5] = false;

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, deselectTwice) {
  const size_t vectorSize = 1;
  SelectivityVector vector(vectorSize);
  SelectivityVector other(vectorSize);

  vector.deselect(other);
  vector.deselect(other);

  std::vector<bool> expected(vectorSize, false);

  ASSERT_NO_FATAL_FAILURE(assertState(expected, vector));
}

TEST(SelectivityVectorTest, setValidRange) {
  const size_t vectorSize = 1000;
  SelectivityVector vector(vectorSize);
  std::vector<bool> expected(vectorSize, true);

  auto setRangeAndAssert =
      [&](size_t startIdxInclusive, size_t endIdxExclusive, bool valid) {
        vector.setValidRange(startIdxInclusive, endIdxExclusive, valid);
        vector.updateBounds();
        std::fill(
            expected.begin() + startIdxInclusive,
            expected.begin() + endIdxExclusive,
            valid);
        assertState(expected, vector);
      };

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(5, 800, false)) << "set to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(100, 300, true))
      << "set inner range to true";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(0, 200, false)) << "start to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(900, vectorSize, false))
      << "end to false";

  ASSERT_NO_FATAL_FAILURE(setRangeAndAssert(0, vectorSize, false))
      << "all to false";
}

TEST(SelectivityVectorTest, setStartEndIdx) {
  SelectivityVector vector(1000 /*vectorSize*/);

  vector.setActiveRange(55, 100);

  EXPECT_EQ(55, vector.begin());
  EXPECT_EQ(100, vector.end());
}

TEST(SelectivityVectorTest, clearAll) {
  auto size = 3;
  // Explicitly set all bits to false.
  // Will call clearAll
  SelectivityVector vector(size, false);

  // Build another vector and set all bits to 0 in brute-force way
  SelectivityVector expected(size);

  for (auto i = 0; i < size; ++i) {
    expected.setValid(i, false);
  }
  expected.updateBounds();

  EXPECT_EQ(expected, vector);
}

TEST(SelectivityVectorTest, setAll) {
  auto size = 3;
  // Initialize with all bits to false
  // Will call clearAll
  SelectivityVector vector(size, false);
  vector.setAll();

  // Build another vector and set all bits to 1 in brute-force way
  SelectivityVector expected(size, true);

  EXPECT_EQ(expected, vector);
}

namespace {

void testEquals(
    bool expectEqual,
    std::function<void(SelectivityVector&)> mungeFunc = nullptr) {
  const size_t vectorSize = 1000;
  SelectivityVector vector1(vectorSize);
  SelectivityVector vector2(vectorSize);

  if (mungeFunc != nullptr) {
    mungeFunc(vector2);
  }

  ASSERT_EQ(expectEqual, vector1 == vector2);
  ASSERT_NE(expectEqual, vector1 != vector2);
}

} // namespace

TEST(SelectivityVectorTest, operatorEquals_allEqual) {
  testEquals(true /*expectEqual*/);
}

TEST(SelectivityVectorTest, operatorEquals_dataNotEqual) {
  testEquals(
      false /*expectEqual*/, [](auto& vector) { vector.setValid(10, false); });
}

TEST(SelectivityVectorTest, operatorEquals_startIdxNotEqual) {
  testEquals(false /*expectEqual*/, [](auto& vector) {
    vector.setActiveRange(10, vector.end());
  });
}

TEST(SelectivityVectorTest, operatorEquals_endIdxNotEqual) {
  testEquals(false /*expectEqual*/, [](auto& vector) {
    vector.setActiveRange(vector.begin(), 10);
  });
}

TEST(SelectivityVectorTest, emptyIterator) {
  SelectivityVector vector(2011);
  vector.clearAll();
  SelectivityIterator empty(vector);
  vector_size_t row = 0;
  EXPECT_FALSE(empty.next(row));
  int32_t count = 0;
  vector.applyToSelected([&count](vector_size_t /* unused */) {
    ++count;
    return true;
  });
  EXPECT_EQ(count, 0);
}

TEST(SelectivityVectorTest, iterator) {
  SelectivityVector vector(2011);
  vector.clearAll();
  std::vector<int32_t> selected;
  vector_size_t row = 0;
  for (int32_t i = 0; i < 1000; ++i) {
    selected.push_back(row);
    vector.setValid(row, true);
    row += (i & 7) | 1;
    if (row > 1500) {
      row += 64;
    }
    if (row > vector.size()) {
      break;
    }
  }
  vector.updateBounds();
  int32_t count = 0;
  SelectivityIterator iter(vector);
  while (iter.next(row)) {
    ASSERT_EQ(row, selected[count]);
    count++;
  }
  ASSERT_EQ(count, selected.size());
  count = 0;
  vector.applyToSelected([selected, &count](int32_t row) {
    EXPECT_EQ(row, selected[count]);
    count++;
    return true;
  });
  ASSERT_EQ(count, selected.size());

  std::vector<uint64_t> contiguous(10);
  bits::fillBits(&contiguous[0], 67, 227, true);
  // Set some trailing bits that are after the range.
  bits::fillBits(&contiguous[0], 240, 540, true);
  SelectivityVector fromBits;
  fromBits.setFromBits(&contiguous[0], 64 * contiguous.size());
  fromBits.setActiveRange(fromBits.begin(), 227);
  EXPECT_EQ(fromBits.begin(), 67);
  EXPECT_EQ(fromBits.end(), 227);
  EXPECT_FALSE(fromBits.isAllSelected());
  count = 0;
  fromBits.applyToSelected([&count](int32_t row) {
    EXPECT_EQ(row, count + 67);
    count++;
    return true;
  });
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
  fromBits.setActiveRange(64, 227);
  EXPECT_FALSE(fromBits.isAllSelected());
  count = 0;
  fromBits.applyToSelected([&count](int32_t row) {
    EXPECT_EQ(row, count + 67);
    count++;
    return true;
  });
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
  count = 0;
  SelectivityIterator iter2(fromBits);
  while (iter2.next(row)) {
    EXPECT_EQ(row, count + 67);
    count++;
  }
  EXPECT_EQ(count, bits::countBits(&contiguous[0], 0, 240));
}

TEST(SelectivityVectorTest, resize) {
  SelectivityVector vector(64, false);
  vector.resize(128, /* value */ true);
  // Ensure last 64 bits are set to 1
  assertIsValid(64, 128, vector, true);

  SelectivityVector rows(64, true);
  rows.resize(128, /* value */ false);
  // Ensure last 64 bits set to false
  assertIsValid(64, 128, rows, false);
  ASSERT_FALSE(rows.isAllSelected());

  // Now test more unusual ranges
  SelectivityVector unusual(37, true);
  assertIsValid(0, 37, unusual, true);
  unusual.resize(63, /* value */ false);
  assertIsValid(0, 37, unusual, true);
  assertIsValid(37, 63, unusual, false);

  // Test for much larger word lengths
  SelectivityVector larger(53, true);
  assertIsValid(0, 53, larger, true);
  larger.resize(656, true);
  assertIsValid(0, 656, larger, true);

  // Check for word length reduction
  larger.resize(53);
  assertIsValid(0, 53, larger, true);
  // Check if all selected is true
  ASSERT_TRUE(larger.isAllSelected());
}

TEST(SelectivityVectorTest, select) {
  SelectivityVector first(64);
  SelectivityVector other(32, false);

  other.select(first);
  assertIsValid(0, 32, other, true);
  ASSERT_TRUE(other.isAllSelected());

  SelectivityVector a(16, false);
  a.resize(33, true);
  SelectivityVector b(32, false);
  b.select(a);
  assertIsValid(16, 32, b, true);

  SelectivityVector empty(0);
  empty.select(first);
  ASSERT_FALSE(empty.isAllSelected());

  SelectivityVector bitAfter2(8);
  bitAfter2.setValid(0, false);
  bitAfter2.setValid(1, false);
  bitAfter2.updateBounds();
  SelectivityVector bitAfterCheck(8, false);
  bitAfterCheck.setValid(3, true);
  bitAfterCheck.select(bitAfter2);
  assertIsValid(2, 8, bitAfterCheck, true);
}

// Sanity check for toString() method. Primarily to ensure the method doesn't
// fail or crash.
TEST(SelectivityVectorTest, toString) {
  SelectivityVector rows(1024);

  // All selected.
  auto summary = rows.toString();
  ASSERT_EQ(
      summary,
      "1024 out of 1024 rows selected between 0 and 1024: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9");

  summary = rows.toString(2);
  ASSERT_EQ(summary, "1024 out of 1024 rows selected between 0 and 1024: 0, 1");

  summary = rows.toString(std::numeric_limits<vector_size_t>::max());
  ASSERT_EQ(
      summary,
      "1024 out of 1024 rows selected between 0 and 1024: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 711, 712, 713, 714, 715, 716, 717, 718, 719, 720, 721, 722, 723, 724, 725, 726, 727, 728, 729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 868, 869, 870, 871, 872, 873, 874, 875, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023");

  summary = rows.toString(0);
  ASSERT_EQ(summary, "1024 out of 1024 rows selected between 0 and 1024");

  ASSERT_THROW(rows.toString(-6), VeloxRuntimeError);

  // None selected.
  rows.clearAll();
  summary = rows.toString();
  ASSERT_EQ(summary, "0 out of 1024 rows selected between 0 and 0");

  summary = rows.toString(5);
  ASSERT_EQ(summary, "0 out of 1024 rows selected between 0 and 0");

  summary = rows.toString(std::numeric_limits<vector_size_t>::max());
  ASSERT_EQ(summary, "0 out of 1024 rows selected between 0 and 0");

  // Some selected.
  for (auto i = 0; i < rows.size(); i += 7) {
    rows.setValid(i, true);
  }
  rows.updateBounds();

  summary = rows.toString();
  ASSERT_EQ(
      summary,
      "147 out of 1024 rows selected between 0 and 1023: 0, 7, 14, 21, 28, 35, 42, 49, 56, 63");

  summary = rows.toString(3);
  ASSERT_EQ(
      summary, "147 out of 1024 rows selected between 0 and 1023: 0, 7, 14");

  summary = rows.toString(std::numeric_limits<vector_size_t>::max());
  ASSERT_EQ(
      summary,
      "147 out of 1024 rows selected between 0 and 1023: 0, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98, 105, 112, 119, 126, 133, 140, 147, 154, 161, 168, 175, 182, 189, 196, 203, 210, 217, 224, 231, 238, 245, 252, 259, 266, 273, 280, 287, 294, 301, 308, 315, 322, 329, 336, 343, 350, 357, 364, 371, 378, 385, 392, 399, 406, 413, 420, 427, 434, 441, 448, 455, 462, 469, 476, 483, 490, 497, 504, 511, 518, 525, 532, 539, 546, 553, 560, 567, 574, 581, 588, 595, 602, 609, 616, 623, 630, 637, 644, 651, 658, 665, 672, 679, 686, 693, 700, 707, 714, 721, 728, 735, 742, 749, 756, 763, 770, 777, 784, 791, 798, 805, 812, 819, 826, 833, 840, 847, 854, 861, 868, 875, 882, 889, 896, 903, 910, 917, 924, 931, 938, 945, 952, 959, 966, 973, 980, 987, 994, 1001, 1008, 1015, 1022");
}

} // namespace test
} // namespace velox
} // namespace facebook
