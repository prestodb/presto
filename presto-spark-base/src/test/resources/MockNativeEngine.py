#!/usr/bin/python3

import os
import sys
import struct

# Fast transform init frame
sys.stdout.write("NATIVE")
sys.stdout.flush()
# Fast transform version
sys.stdout.buffer.write(struct.pack('<I', 1))
sys.stdout.flush()

# header ack
header = sys.stdin.read(4)
# version ack
version = sys.stdin.read(4)
frameLength = sys.stdin.read(4)
sys.stdin.read(1)

sys.exit(0)
