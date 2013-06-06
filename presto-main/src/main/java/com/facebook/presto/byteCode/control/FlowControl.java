package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.ByteCodeNode;

public interface FlowControl extends ByteCodeNode {
    String getComment();
}
