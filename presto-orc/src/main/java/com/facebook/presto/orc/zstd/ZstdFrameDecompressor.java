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
package com.facebook.presto.orc.zstd;

import io.airlift.compress.MalformedInputException;
import io.airlift.slice.Slice;
import io.airlift.slice.UnsafeSliceFactory;
import io.airlift.slice.XxHash64;

import java.util.Arrays;

import static com.facebook.presto.orc.zstd.BitStream.peekBits;
import static com.facebook.presto.orc.zstd.UnsafeUtil.UNSAFE;
import static com.facebook.presto.orc.zstd.Util.fail;
import static com.facebook.presto.orc.zstd.Util.mask;
import static com.facebook.presto.orc.zstd.Util.verify;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class ZstdFrameDecompressor
{
    private static final int MIN_MATCH = 3;

    private static final int[] DEC_32_TABLE = {4, 1, 2, 1, 4, 4, 4, 4};
    private static final int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private static final int MAGIC_NUMBER = 0xFD2FB528; // v0.5

    private static final int MIN_SEQUENCES_SIZE = 1;
    private static final int MIN_BLOCK_SIZE = 1 // block type tag
            + 1 // min size of raw or rle length header
            + MIN_SEQUENCES_SIZE;

    private static final int MAX_BLOCK_SIZE = 128 * 1024;

    private static final int MIN_WINDOW_LOG = 10;
    private static final int MAX_WINDOW_SIZE = 1 << 23;

    public static final int SIZE_OF_BYTE = 1;
    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;

    private static final long SIZE_OF_BLOCK_HEADER = 3;

    // block types
    private static final int RAW_BLOCK = 0;
    private static final int RLE_BLOCK = 1;
    private static final int COMPRESSED_BLOCK = 2;

    // literal block types
    private static final int RAW_LITERALS_BLOCK = 0;
    private static final int RLE_LITERALS_BLOCK = 1;
    private static final int COMPRESSED_LITERALS_BLOCK = 2;
    private static final int REPEAT_STATS_LITERALS_BLOCK = 3;

    private static final int LONG_NUMBER_OF_SEQUENCES = 0x7F00;

    private static final int MAX_LITERALS_LENGTH_SYMBOL = 35;
    private static final int MAX_MATCH_LENGTH_SYMBOL = 52;
    private static final int MAX_OFFSET_CODE_SYMBOL = 28;

    private static final int LITERALS_LENGTH_FSE_LOG = 9;
    private static final int MATCH_LENGTH_FSE_LOG = 9;
    private static final int OFFSET_CODES_FSE_LOG = 8;

    private static final int SET_BASIC = 0;
    private static final int SET_RLE = 1;
    private static final int SET_COMPRESSED = 2;
    private static final int SET_REPEAT = 3;

    private static final int[] LITERALS_LENGTH_BASE = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 0x80, 0x100, 0x200, 0x400, 0x800, 0x1000,
            0x2000, 0x4000, 0x8000, 0x10000};

    private static final int[] MATCH_LENGTH_BASE = {
            3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
            35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 0x83, 0x103, 0x203, 0x403, 0x803,
            0x1003, 0x2003, 0x4003, 0x8003, 0x10003};

    private static final int[] OFFSET_CODES_BASE = {
            0, 1, 1, 5, 0xD, 0x1D, 0x3D, 0x7D,
            0xFD, 0x1FD, 0x3FD, 0x7FD, 0xFFD, 0x1FFD, 0x3FFD, 0x7FFD,
            0xFFFD, 0x1FFFD, 0x3FFFD, 0x7FFFD, 0xFFFFD, 0x1FFFFD, 0x3FFFFD, 0x7FFFFD,
            0xFFFFFD, 0x1FFFFFD, 0x3FFFFFD, 0x7FFFFFD, 0xFFFFFFD};

    private static final int[] LITERALS_LENGTH_BITS = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16};

    private static final int[] MATCH_LENGTH_BITS = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11,
            12, 13, 14, 15, 16};

    private static final FiniteStateEntropy.Table DEFAULT_LITERALS_LENGTH_TABLE = new FiniteStateEntropy.Table(
            6,
            new int[] {
                    0, 16, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 32, 0, 0, 32, 0, 32, 0, 32, 0, 0, 32, 0, 32, 0, 32, 0, 0, 16, 32, 0, 0, 48, 16, 32, 32, 32,
                    32, 32, 32, 32, 32, 0, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0},
            new byte[] {
                    0, 0, 1, 3, 4, 6, 7, 9, 10, 12, 14, 16, 18, 19, 21, 22, 24, 25, 26, 27, 29, 31, 0, 1, 2, 4, 5, 7, 8, 10, 11, 13, 16, 17, 19, 20, 22, 23, 25, 25, 26, 28, 30, 0,
                    1, 2, 3, 5, 6, 8, 9, 11, 12, 15, 17, 18, 20, 21, 23, 24, 35, 34, 33, 32},
            new byte[] {
                    4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 5, 5, 5, 5, 5, 5, 4, 4, 5, 6, 6, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5,
                    6, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6});

    private static final FiniteStateEntropy.Table DEFAULT_OFFSET_CODES_TABLE = new FiniteStateEntropy.Table(
            5,
            new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 16, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0},
            new byte[] {0, 6, 9, 15, 21, 3, 7, 12, 18, 23, 5, 8, 14, 20, 2, 7, 11, 17, 22, 4, 8, 13, 19, 1, 6, 10, 16, 28, 27, 26, 25, 24},
            new byte[] {5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5});

    private static final FiniteStateEntropy.Table DEFAULT_MATCH_LENGTH_TABLE = new FiniteStateEntropy.Table(
            6,
            new int[] {
                    0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 32, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 48, 16, 32, 32, 32, 32,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            new byte[] {
                    0, 1, 2, 3, 5, 6, 8, 10, 13, 16, 19, 22, 25, 28, 31, 33, 35, 37, 39, 41, 43, 45, 1, 2, 3, 4, 6, 7, 9, 12, 15, 18, 21, 24, 27, 30, 32, 34, 36, 38, 40, 42, 44, 1,
                    1, 2, 4, 5, 7, 8, 11, 14, 17, 20, 23, 26, 29, 52, 51, 50, 49, 48, 47, 46},
            new byte[] {
                    6, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6});

    private final byte[] literals = new byte[MAX_BLOCK_SIZE + SIZE_OF_LONG]; // extra space to allow for long-at-a-time copy

    // current buffer containing literals
    private Object literalsBase;
    private long literalsAddress;
    private long literalsLimit;

    private final int[] previousOffsets = new int[3];

    private final FiniteStateEntropy.Table literalsLengthTable = new FiniteStateEntropy.Table(LITERALS_LENGTH_FSE_LOG);
    private final FiniteStateEntropy.Table offsetCodesTable = new FiniteStateEntropy.Table(OFFSET_CODES_FSE_LOG);
    private final FiniteStateEntropy.Table matchLengthTable = new FiniteStateEntropy.Table(MATCH_LENGTH_FSE_LOG);

    private FiniteStateEntropy.Table currentLiteralsLengthTable;
    private FiniteStateEntropy.Table currentOffsetCodesTable;
    private FiniteStateEntropy.Table currentMatchLengthTable;

    private final Huffman huffman = new Huffman();
    private final FseTableReader fse = new FseTableReader();

    public int decompress(
            final Object inputBase,
            final long inputAddress,
            final long inputLimit,
            final Object outputBase,
            final long outputAddress,
            final long outputLimit)
    {
        if (outputAddress == outputLimit) {
            return 0;
        }

        reset();

        long input = inputAddress;
        long output = outputAddress;

        input += verifyMagic(inputBase, inputAddress, inputLimit);

        FrameHeader frameHeader = readFrameHeader(inputBase, input, inputLimit);
        input += frameHeader.headerSize;

        boolean lastBlock;
        do {
            verify(input + SIZE_OF_BLOCK_HEADER <= inputLimit, input, "Not enough input bytes");

            // read block header
            int header = UNSAFE.getInt(inputBase, input) & 0xFF_FFFF;
            input += SIZE_OF_BLOCK_HEADER;

            lastBlock = (header & 1) != 0;
            int blockType = (header >>> 1) & 0b11;
            int blockSize = (header >>> 3) & 0x1F_FFFF; // 21 bits

            int decodedSize;
            switch (blockType) {
                case RAW_BLOCK:
                    verify(inputAddress + blockSize <= inputLimit, input, "Not enough input bytes");
                    decodedSize = decodeRawBlock(inputBase, input, blockSize, outputBase, output, outputLimit);
                    input += blockSize;
                    break;
                case RLE_BLOCK:
                    verify(inputAddress + 1 <= inputLimit, input, "Not enough input bytes");
                    decodedSize = decodeRleBlock(blockSize, inputBase, input, outputBase, output, outputLimit);
                    input += 1;
                    break;
                case COMPRESSED_BLOCK:
                    verify(inputAddress + blockSize <= inputLimit, input, "Not enough input bytes");
                    decodedSize = decodeCompressedBlock(inputBase, input, blockSize, outputBase, output, outputLimit, frameHeader.windowSize);
                    input += blockSize;
                    break;
                default:
                    throw fail(input, "Invalid block type");
            }

            output += decodedSize;
        }
        while (!lastBlock);

        if (frameHeader.hasChecksum) {
            Slice outputSlice = UnsafeSliceFactory.getInstance().newSlice(outputBase, outputAddress, (int) (outputLimit - outputAddress));
            long hash = XxHash64.hash(0, outputSlice);

            int checksum = UNSAFE.getInt(inputBase, input);
            if (checksum != (int) hash) {
                throw new MalformedInputException(input, String.format("Bad checksum. Expected: %s, actual: %s", Integer.toHexString(checksum), Integer.toHexString((int) hash)));
            }
        }

        return (int) (output - outputAddress);
    }

    private void reset()
    {
        previousOffsets[0] = 1;
        previousOffsets[1] = 4;
        previousOffsets[2] = 8;

        currentLiteralsLengthTable = null;
        currentOffsetCodesTable = null;
        currentMatchLengthTable = null;
    }

    private static int decodeRawBlock(Object inputBase, long inputAddress, int blockSize, Object outputBase, long outputAddress, long outputLimit)
    {
        verify(outputAddress + blockSize <= outputLimit, inputAddress, "Output buffer too small");

        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress, blockSize);
        return blockSize;
    }

    private static int decodeRleBlock(int size, Object inputBase, long inputAddress, Object outputBase, long outputAddress, long outputLimit)
    {
        verify(outputAddress + size <= outputLimit, inputAddress, "Output buffer too small");

        long output = outputAddress;
        long value = UNSAFE.getByte(inputBase, inputAddress) & 0xFFL;

        int remaining = size;
        if (remaining >= SIZE_OF_LONG) {
            long packed = value
                    | (value << 8)
                    | (value << 16)
                    | (value << 24)
                    | (value << 32)
                    | (value << 40)
                    | (value << 48)
                    | (value << 56);

            do {
                UNSAFE.putLong(outputBase, output, packed);
                output += SIZE_OF_LONG;
                remaining -= SIZE_OF_LONG;
            }
            while (remaining >= SIZE_OF_LONG);
        }

        for (int i = 0; i < remaining; i++) {
            UNSAFE.putByte(outputBase, output, (byte) value);
            output++;
        }

        return size;
    }

    private int decodeCompressedBlock(Object inputBase, final long inputAddress, int blockSize, Object outputBase, long outputAddress, long outputLimit, int windowSize)
    {
        long inputLimit = inputAddress + blockSize;
        long input = inputAddress;

        verify(blockSize <= MAX_BLOCK_SIZE, input, "Expected match length table to be present");
        verify(blockSize >= MIN_BLOCK_SIZE, input, "Compressed block size too small");

        // decode literals
        int literalsBlockType = UNSAFE.getByte(inputBase, input) & 0b11;

        switch (literalsBlockType) {
            case RAW_LITERALS_BLOCK: {
                input += decodeRawLiterals(inputBase, input, inputLimit);
                break;
            }
            case RLE_LITERALS_BLOCK: {
                input += decodeRleLiterals(inputBase, input, blockSize);
                break;
            }
            case REPEAT_STATS_LITERALS_BLOCK:
                verify(huffman.isLoaded(), input, "Dictionary is corrupted");
            case COMPRESSED_LITERALS_BLOCK: {
                input += decodeCompressedLiterals(inputBase, input, blockSize, literalsBlockType);
                break;
            }
            default:
                throw fail(input, "Invalid literals block encoding type");
        }

        verify(windowSize <= MAX_WINDOW_SIZE, input, "Window size too large (not yet supported)");

        return decompressSequences(
                inputBase, input, inputAddress + blockSize,
                outputBase, outputAddress, outputLimit,
                literalsBase, literalsAddress, literalsLimit);
    }

    private int decompressSequences(
            final Object inputBase, final long inputAddress, final long inputLimit,
            final Object outputBase, final long outputAddress, final long outputLimit,
            final Object literalsBase, final long literalsAddress, final long literalsLimit)
    {
        final long fastOutputLimit = outputLimit - SIZE_OF_LONG;

        long input = inputAddress;
        long output = outputAddress;

        long literalsInput = literalsAddress;

        int size = (int) (inputLimit - inputAddress);
        verify(size >= MIN_SEQUENCES_SIZE, input, "Not enough input bytes");

        // decode header
        int sequenceCount = UNSAFE.getByte(inputBase, input++) & 0xFF;
        if (sequenceCount != 0) {
            if (sequenceCount == 255) {
                verify(input + SIZE_OF_SHORT <= inputLimit, input, "Not enough input bytes");
                sequenceCount = (UNSAFE.getShort(inputBase, input) & 0xFFFF) + LONG_NUMBER_OF_SEQUENCES;
                input += SIZE_OF_SHORT;
            }
            else if (sequenceCount > 127) {
                verify(input < inputLimit, input, "Not enough input bytes");
                sequenceCount = ((sequenceCount - 128) << 8) + (UNSAFE.getByte(inputBase, input++) & 0xFF);
            }

            verify(input + SIZE_OF_INT <= inputLimit, input, "Not enough input bytes");

            byte type = UNSAFE.getByte(inputBase, input++);

            int literalsLengthType = (type & 0xFF) >>> 6;
            int offsetCodesType = (type >>> 4) & 0b11;
            int matchLengthType = (type >>> 2) & 0b11;

            input = computeLiteralsTable(literalsLengthType, inputBase, input, inputLimit);
            input = computeOffsetsTable(offsetCodesType, inputBase, input, inputLimit);
            input = computeMatchLengthTable(matchLengthType, inputBase, input, inputLimit);

            // decompress sequences
            BitStream.Initializer initializer = new BitStream.Initializer(inputBase, input, inputLimit);
            initializer.initialize();
            int bitsConsumed = initializer.getBitsConsumed();
            long bits = initializer.getBits();
            long currentAddress = initializer.getCurrentAddress();

            FiniteStateEntropy.Table currentLiteralsLengthTable = this.currentLiteralsLengthTable;
            FiniteStateEntropy.Table currentOffsetCodesTable = this.currentOffsetCodesTable;
            FiniteStateEntropy.Table currentMatchLengthTable = this.currentMatchLengthTable;

            int literalsLengthState = (int) peekBits(bitsConsumed, bits, currentLiteralsLengthTable.log2Size);
            bitsConsumed += currentLiteralsLengthTable.log2Size;

            int offsetCodesState = (int) peekBits(bitsConsumed, bits, currentOffsetCodesTable.log2Size);
            bitsConsumed += currentOffsetCodesTable.log2Size;

            int matchLengthState = (int) peekBits(bitsConsumed, bits, currentMatchLengthTable.log2Size);
            bitsConsumed += currentMatchLengthTable.log2Size;

            int[] previousOffsets = this.previousOffsets;

            byte[] literalsLengthNumbersOfBits = currentLiteralsLengthTable.numberOfBits;
            int[] literalsLengthNewStates = currentLiteralsLengthTable.newState;
            byte[] literalsLengthSymbols = currentLiteralsLengthTable.symbol;

            byte[] matchLengthNumbersOfBits = currentMatchLengthTable.numberOfBits;
            int[] matchLengthNewStates = currentMatchLengthTable.newState;
            byte[] matchLengthSymbols = currentMatchLengthTable.symbol;

            byte[] offsetCodesNumbersOfBits = currentOffsetCodesTable.numberOfBits;
            int[] offsetCodesNewStates = currentOffsetCodesTable.newState;
            byte[] offsetCodesSymbols = currentOffsetCodesTable.symbol;

            while (sequenceCount > 0) {
                sequenceCount--;

                BitStream.Loader loader = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
                loader.load();
                bitsConsumed = loader.getBitsConsumed();
                bits = loader.getBits();
                currentAddress = loader.getCurrentAddress();
                if (loader.isOverflow()) {
                    verify(sequenceCount == 0, input, "Not all sequences were consumed");
                    break;
                }

                // decode sequence
                int literalsLengthCode = literalsLengthSymbols[literalsLengthState];
                int matchLengthCode = matchLengthSymbols[matchLengthState];
                int offsetCode = offsetCodesSymbols[offsetCodesState];

                int literalsLengthBits = LITERALS_LENGTH_BITS[literalsLengthCode];
                int matchLengthBits = MATCH_LENGTH_BITS[matchLengthCode];
                int offsetBits = offsetCode;

                int offset = OFFSET_CODES_BASE[offsetCode];
                if (offsetCode > 0) {
                    offset += peekBits(bitsConsumed, bits, offsetBits);
                    bitsConsumed += offsetBits;
                }

                if (offsetCode <= 1) {
                    if (literalsLengthCode == 0) {
                        offset++;
                    }

                    if (offset != 0) {
                        int temp;
                        if (offset == 3) {
                            temp = previousOffsets[0] - 1;
                        }
                        else {
                            temp = previousOffsets[offset];
                        }

                        if (temp == 0) {
                            temp = 1;
                        }

                        if (offset != 1) {
                            previousOffsets[2] = previousOffsets[1];
                        }
                        previousOffsets[1] = previousOffsets[0];
                        previousOffsets[0] = temp;

                        offset = temp;
                    }
                    else {
                        offset = previousOffsets[0];
                    }
                }
                else {
                    previousOffsets[2] = previousOffsets[1];
                    previousOffsets[1] = previousOffsets[0];
                    previousOffsets[0] = offset;
                }

                int matchLength = MATCH_LENGTH_BASE[matchLengthCode];
                if (matchLengthCode > 31) {
                    matchLength += peekBits(bitsConsumed, bits, matchLengthBits);
                    bitsConsumed += matchLengthBits;
                }

                int literalsLength = LITERALS_LENGTH_BASE[literalsLengthCode];
                if (literalsLengthCode > 15) {
                    literalsLength += peekBits(bitsConsumed, bits, literalsLengthBits);
                    bitsConsumed += literalsLengthBits;
                }

                int totalBits = literalsLengthBits + matchLengthBits + offsetBits;
                if (totalBits > 64 - 7 - (LITERALS_LENGTH_FSE_LOG + MATCH_LENGTH_FSE_LOG + OFFSET_CODES_FSE_LOG)) {
                    BitStream.Loader loader1 = new BitStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
                    loader1.load();

                    bitsConsumed = loader1.getBitsConsumed();
                    bits = loader1.getBits();
                    currentAddress = loader1.getCurrentAddress();
                }

                int numberOfBits;

                numberOfBits = literalsLengthNumbersOfBits[literalsLengthState];
                literalsLengthState = (int) (literalsLengthNewStates[literalsLengthState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 9 bits
                bitsConsumed += numberOfBits;

                numberOfBits = matchLengthNumbersOfBits[matchLengthState];
                matchLengthState = (int) (matchLengthNewStates[matchLengthState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 9 bits
                bitsConsumed += numberOfBits;

                numberOfBits = offsetCodesNumbersOfBits[offsetCodesState];
                offsetCodesState = (int) (offsetCodesNewStates[offsetCodesState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 8 bits
                bitsConsumed += numberOfBits;

                final long literalOutputLimit = output + literalsLength;
                final long matchOutputLimit = literalOutputLimit + matchLength;

                verify(matchOutputLimit <= outputLimit, input, "Output buffer too small");
                verify(literalsInput + literalsLength <= literalsLimit, input, "Input is corrupted");

                long matchAddress = literalOutputLimit - offset;

                if (literalOutputLimit > fastOutputLimit) {
                    executeLastSequence(outputBase, output, literalOutputLimit, matchOutputLimit, fastOutputLimit, literalsInput, matchAddress);
                }
                else {
                    // copy literals. literalOutputLimit <= fastOutputLimit, so we can copy
                    // long at a time with over-copy
                    output = copyLiterals(outputBase, literalsBase, output, literalsInput, literalOutputLimit);
                    copyMatch(outputBase, fastOutputLimit, output, offset, matchOutputLimit, matchAddress);
                }
                output = matchOutputLimit;
                literalsInput += literalsLength;
            }
        }

        // last literal segment
        output = copyLastLiteral(outputBase, literalsBase, literalsLimit, output, literalsInput);

        return (int) (output - outputAddress);
    }

    private long copyLastLiteral(Object outputBase, Object literalsBase, long literalsLimit, long output, long literalsInput)
    {
        long lastLiteralsSize = literalsLimit - literalsInput;
        UNSAFE.copyMemory(literalsBase, literalsInput, outputBase, output, lastLiteralsSize);
        output += lastLiteralsSize;
        return output;
    }

    private void copyMatch(Object outputBase, long fastOutputLimit, long output, int offset, long matchOutputLimit, long matchAddress)
    {
        matchAddress = copyMatchHead(outputBase, output, offset, matchAddress);
        output += SIZE_OF_LONG;

        copyMatchTail(outputBase, fastOutputLimit, output, matchOutputLimit, matchAddress);
    }

    private void copyMatchTail(Object outputBase, long fastOutputLimit, long output, long matchOutputLimit, long matchAddress)
    {
        if (matchOutputLimit > fastOutputLimit) {
            while (output < fastOutputLimit) {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            while (output < matchOutputLimit) {
                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
            }
        }
        else {
            while (output < matchOutputLimit) {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }
        }
    }

    private long copyMatchHead(Object outputBase, long output, int offset, long matchAddress)
    {
        // copy match
        if (offset < 8) {
            // 8 bytes apart so that we can copy long-at-a-time below
            int increment32 = DEC_32_TABLE[offset];
            int decrement64 = DEC_64_TABLE[offset];

            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
            UNSAFE.putByte(outputBase, output + 1, UNSAFE.getByte(outputBase, matchAddress + 1));
            UNSAFE.putByte(outputBase, output + 2, UNSAFE.getByte(outputBase, matchAddress + 2));
            UNSAFE.putByte(outputBase, output + 3, UNSAFE.getByte(outputBase, matchAddress + 3));
            matchAddress += increment32;

            UNSAFE.putInt(outputBase, output + 4, UNSAFE.getInt(outputBase, matchAddress));
            matchAddress -= decrement64;
        }
        else {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
            matchAddress += SIZE_OF_LONG;
        }
        return matchAddress;
    }

    private long copyLiterals(Object outputBase, Object literalsBase, long output, long literalsInput, long literalOutputLimit)
    {
        long literalInput = literalsInput;
        do {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(literalsBase, literalInput));
            output += SIZE_OF_LONG;
            literalInput += SIZE_OF_LONG;
        }
        while (output < literalOutputLimit);
        output = literalOutputLimit; // correction in case we over-copied
        return output;
    }

    private long computeMatchLengthTable(int matchLengthType, Object inputBase, long input, long inputLimit)
    {
        switch (matchLengthType) {
            case SET_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
                verify(value <= MAX_MATCH_LENGTH_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.buildRleTable(matchLengthTable, value);
                currentMatchLengthTable = matchLengthTable;
                break;
            case SET_BASIC:
                currentMatchLengthTable = DEFAULT_MATCH_LENGTH_TABLE;
                break;
            case SET_REPEAT:
                verify(currentMatchLengthTable != null, input, "Expected match length table to be present");
                break;
            case SET_COMPRESSED:
                input += fse.readFseTable(matchLengthTable, inputBase, input, inputLimit, MAX_MATCH_LENGTH_SYMBOL, MATCH_LENGTH_FSE_LOG);
                currentMatchLengthTable = matchLengthTable;
                break;
            default:
                throw fail(input, "Invalid match length encoding type");
        }
        return input;
    }

    private long computeOffsetsTable(int offsetCodesType, Object inputBase, long input, long inputLimit)
    {
        switch (offsetCodesType) {
            case SET_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
                verify(value <= MAX_OFFSET_CODE_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.buildRleTable(offsetCodesTable, value);
                currentOffsetCodesTable = offsetCodesTable;
                break;
            case SET_BASIC:
                currentOffsetCodesTable = DEFAULT_OFFSET_CODES_TABLE;
                break;
            case SET_REPEAT:
                verify(currentOffsetCodesTable != null, input, "Expected match length table to be present");
                break;
            case SET_COMPRESSED:
                input += fse.readFseTable(offsetCodesTable, inputBase, input, inputLimit, MAX_OFFSET_CODE_SYMBOL, OFFSET_CODES_FSE_LOG);
                currentOffsetCodesTable = offsetCodesTable;
                break;
            default:
                throw fail(input, "Invalid offset code encoding type");
        }
        return input;
    }

    private long computeLiteralsTable(int literalsLengthType, Object inputBase, long input, long inputLimit)
    {
        switch (literalsLengthType) {
            case SET_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
                verify(value <= MAX_LITERALS_LENGTH_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.buildRleTable(literalsLengthTable, value);
                currentLiteralsLengthTable = literalsLengthTable;
                break;
            case SET_BASIC:
                currentLiteralsLengthTable = DEFAULT_LITERALS_LENGTH_TABLE;
                break;
            case SET_REPEAT:
                verify(currentLiteralsLengthTable != null, input, "Expected match length table to be present");
                break;
            case SET_COMPRESSED:
                input += fse.readFseTable(literalsLengthTable, inputBase, input, inputLimit, MAX_LITERALS_LENGTH_SYMBOL, LITERALS_LENGTH_FSE_LOG);
                currentLiteralsLengthTable = literalsLengthTable;
                break;
            default:
                throw fail(input, "Invalid literals length encoding type");
        }
        return input;
    }

    private void executeLastSequence(Object outputBase, long output, long literalOutputLimit, long matchOutputLimit, long fastOutputLimit, long literalInput, long matchAddress)
    {
        // copy literals
        if (output < fastOutputLimit) {
            // wild copy
            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(literalsBase, literalInput));
                output += SIZE_OF_LONG;
                literalInput += SIZE_OF_LONG;
            }
            while (output < fastOutputLimit);

            literalInput -= output - fastOutputLimit;
            output = fastOutputLimit;
        }

        while (output < literalOutputLimit) {
            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(literalsBase, literalInput));
            output++;
            literalInput++;
        }

        // copy match
        while (output < matchOutputLimit) {
            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
            output++;
            matchAddress++;
        }
    }

    private int decodeCompressedLiterals(Object inputBase, final long inputAddress, int blockSize, int literalsBlockType)
    {
        long input = inputAddress;
        verify(blockSize >= 5, input, "Not enough input bytes");

        // compressed
        int compressedSize;
        int uncompressedSize;
        boolean singleStream = false;
        int headerSize;
        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;
        switch (type) {
            case 0:
                singleStream = true;
            case 1: {
                int header = UNSAFE.getInt(inputBase, input);

                headerSize = 3;
                uncompressedSize = (header >>> 4) & mask(10);
                compressedSize = (header >>> 14) & mask(10);
                break;
            }
            case 2: {
                int header = UNSAFE.getInt(inputBase, input);

                headerSize = 4;
                uncompressedSize = (header >>> 4) & mask(14);
                compressedSize = (header >>> 18) & mask(14);
                break;
            }
            case 3: {
                // read 5 little-endian bytes
                long header = UNSAFE.getByte(inputBase, input) & 0xFF |
                        (UNSAFE.getInt(inputBase, input + 1) & 0xFFFF_FFFFL) << 8;

                headerSize = 5;
                uncompressedSize = (int) ((header >>> 4) & mask(18));
                compressedSize = (int) ((header >>> 22) & mask(18));
                break;
            }
            default:
                throw fail(input, "Invalid literals header size type");
        }

        verify(uncompressedSize <= MAX_BLOCK_SIZE, input, "Block exceeds maximum size");
        verify(headerSize + compressedSize <= blockSize, input, "Input is corrupted");

        input += headerSize;

        long inputLimit = input + compressedSize;
        if (literalsBlockType != REPEAT_STATS_LITERALS_BLOCK) {
            input += huffman.readTable(inputBase, input, compressedSize);
        }

        literalsBase = literals;
        literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        literalsLimit = ARRAY_BYTE_BASE_OFFSET + uncompressedSize;

        if (singleStream) {
            huffman.decodeSingleStream(inputBase, input, inputLimit, literals, literalsAddress, literalsLimit);
        }
        else {
            huffman.decode4Streams(inputBase, input, inputLimit, literals, literalsAddress, literalsLimit);
        }

        return headerSize + compressedSize;
    }

    private int decodeRleLiterals(Object inputBase, final long inputAddress, int blockSize)
    {
        long input = inputAddress;
        int outputSize;

        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;
        switch (type) {
            case 0:
            case 2:
                outputSize = (UNSAFE.getByte(inputBase, input) & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                outputSize = (UNSAFE.getShort(inputBase, input) & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // we need at least 4 bytes (3 for the header, 1 for the payload)
                verify(blockSize >= SIZE_OF_INT, input, "Not enough input bytes");
                outputSize = (UNSAFE.getInt(inputBase, input) & 0xFF_FFFF) >>> 4;
                input += 3;
                break;
            default:
                throw fail(input, "Invalid RLE literals header encoding type");
        }

        verify(outputSize <= MAX_BLOCK_SIZE, input, "Output exceeds maximum block size");

        byte value = UNSAFE.getByte(inputBase, input++);
        Arrays.fill(literals, 0, outputSize + SIZE_OF_LONG, value);

        literalsBase = literals;
        literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        literalsLimit = ARRAY_BYTE_BASE_OFFSET + outputSize;

        return (int) (input - inputAddress);
    }

    private int decodeRawLiterals(Object inputBase, final long inputAddress, long inputLimit)
    {
        long input = inputAddress;
        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;

        int literalSize;
        switch (type) {
            case 0:
            case 2:
                literalSize = (UNSAFE.getByte(inputBase, input) & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                literalSize = (UNSAFE.getShort(inputBase, input) & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // read 3 little-endian bytes
                int header = ((UNSAFE.getByte(inputBase, input) & 0xFF) |
                        ((UNSAFE.getShort(inputBase, input + 1) & 0xFFFF) << 8));

                literalSize = header >>> 4;
                input += 3;
                break;
            default:
                throw fail(input, "Invalid raw literals header encoding type");
        }

        verify(input + literalSize <= inputLimit, input, "Not enough input bytes");

        // Set literals pointer to [input, literalSize], but only if we can copy 8 bytes at a time during sequence decoding
        // Otherwise, copy literals into buffer that's big enough to guarantee that
        if (literalSize > (inputLimit - input) - SIZE_OF_LONG) {
            literalsBase = literals;
            literalsAddress = ARRAY_BYTE_BASE_OFFSET;
            literalsLimit = ARRAY_BYTE_BASE_OFFSET + literalSize;

            UNSAFE.copyMemory(inputBase, input, literals, literalsAddress, literalSize);
            Arrays.fill(literals, literalSize, literalSize + SIZE_OF_LONG, (byte) 0);
        }
        else {
            literalsBase = inputBase;
            literalsAddress = input;
            literalsLimit = literalsAddress + literalSize;
        }
        input += literalSize;

        return (int) (input - inputAddress);
    }

    private static FrameHeader readFrameHeader(final Object inputBase, final long inputAddress, final long inputLimit)
    {
        long input = inputAddress;
        verify(input < inputLimit, input, "Not enough input bytes");

        int frameHeaderDescriptor = UNSAFE.getByte(inputBase, input++) & 0xFF;
        boolean singleSegment = (frameHeaderDescriptor & 0b100000) != 0;
        int dictionaryDescriptor = frameHeaderDescriptor & 0b11;
        int contentSizeDescriptor = frameHeaderDescriptor >>> 6;

        int headerSize = 1 +
                (singleSegment ? 0 : 1) +
                (dictionaryDescriptor == 0 ? 0 : (1 << (dictionaryDescriptor - 1))) +
                (contentSizeDescriptor == 0 ? (singleSegment ? 1 : 0) : (1 << contentSizeDescriptor));

        verify(headerSize <= inputLimit - inputAddress, input, "Not enough input bytes");

        // decode window size
        int windowSize = -1;
        if (!singleSegment) {
            int windowDescriptor = UNSAFE.getByte(inputBase, input++) & 0xFF;
            int exponent = windowDescriptor >>> 3;
            int mantissa = windowDescriptor & 0b111;

            int base = 1 << (MIN_WINDOW_LOG + exponent);
            windowSize = base + (base / 8) * mantissa;
        }

        // decode dictionary id
        long dictionaryId = -1;
        switch (dictionaryDescriptor) {
            case 1:
                dictionaryId = UNSAFE.getByte(inputBase, input) & 0xFF;
                input += SIZE_OF_BYTE;
                break;
            case 2:
                dictionaryId = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                input += SIZE_OF_SHORT;
                break;
            case 3:
                dictionaryId = UNSAFE.getInt(inputBase, input) & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
        }
        verify(dictionaryId == -1, input, "Custom dictionaries not supported");

        // decode content size
        long contentSize = -1;
        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    contentSize = UNSAFE.getByte(inputBase, input) & 0xFF;
                    input += SIZE_OF_BYTE;
                }
                break;
            case 1:
                contentSize = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                contentSize += 256;
                input += SIZE_OF_SHORT;
                break;
            case 2:
                contentSize = UNSAFE.getInt(inputBase, input) & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
            case 3:
                contentSize = UNSAFE.getLong(inputBase, input);
                input += SIZE_OF_LONG;
                break;
        }

        boolean hasChecksum = (frameHeaderDescriptor & 0b100) != 0;

        return new FrameHeader(
                input - inputAddress,
                windowSize,
                contentSize,
                dictionaryId,
                hasChecksum);
    }

    public static long getDecompressedSize(final Object inputBase, final long inputAddress, final long inputLimit)
    {
        long input = inputAddress;
        input += verifyMagic(inputBase, input, inputLimit);
        return readFrameHeader(inputBase, input, inputLimit).contentSize;
    }

    private static int verifyMagic(Object inputBase, long inputAddress, long inputLimit)
    {
        verify(inputLimit - inputAddress >= 4, inputAddress, "Not enough input bytes");

        int magic = UNSAFE.getInt(inputBase, inputAddress);
        if (magic != MAGIC_NUMBER) {
            throw new MalformedInputException(inputAddress, "Invalid magic prefix: " + Integer.toHexString(magic));
        }

        return SIZE_OF_INT;
    }
}
