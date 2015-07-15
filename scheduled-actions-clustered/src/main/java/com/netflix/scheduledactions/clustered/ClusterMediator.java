/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.scheduledactions.clustered;

import com.netflix.scheduledactions.clustered.notifications.ActionInstanceMessage;
import com.netflix.scheduledactions.clustered.notifications.ExecutionMessage;

import java.util.List;

/**
 * @author sthadeshwar
 */
public interface ClusterMediator {

    public void sendActionMessage(ActionInstanceMessage actionInstanceMessage);
    public List<ActionInstanceMessage> getAllActionMessages();
    public void sendExecutionMessage(ExecutionMessage executionMessage);
    public List<ExecutionMessage> getAllExecutionMessages();

    public boolean isExecutingAction(String executionId, String actionInstanceId);
    public boolean shouldExecuteAction(String actionInstanceId, long ttlSeconds);
}
