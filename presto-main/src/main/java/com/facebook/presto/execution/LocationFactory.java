/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;

import java.net.URI;

public interface LocationFactory
{
    URI createQueryLocation(String queryId);

    URI createStageLocation(String queryId, String stageId);

    URI createTaskLocation(Node node, String taskId);
}
