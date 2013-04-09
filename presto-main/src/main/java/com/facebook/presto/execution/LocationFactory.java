/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;

import java.net.URI;

public interface LocationFactory
{
    URI createQueryLocation(QueryId queryId);

    URI createStageLocation(StageId stageId);

    URI createTaskLocation(Node node, TaskId taskId);
}
