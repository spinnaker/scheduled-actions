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

package com.netflix.scheduledactions.clustered.notifications;

import com.netflix.scheduledactions.clustered.ClusterMediator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExecutionStatusChangePollingAgent extends AbstractPollingAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionStatusChangePollingAgent.class);

    private final long pollingIntervalMs;
    private final ExecutionStatusChangeListener executionStatusChangeListener;
    private final ClusterMediator clusterMediator;

    public ExecutionStatusChangePollingAgent(long pollingIntervalMs,
                                             ExecutionStatusChangeListener executionStatusChangeListener,
                                             ClusterMediator clusterMediator) {
        this.pollingIntervalMs = pollingIntervalMs;
        this.executionStatusChangeListener = executionStatusChangeListener;
        this.clusterMediator = clusterMediator;
    }

    @Override
    public String getName() {
        return ActionStatusChangePollingAgent.class.getSimpleName();
    }

    @Override
    public long getIntervalMs() {
        return pollingIntervalMs;
    }

    @Override
    public void execute() {
        try {
            List<ExecutionMessage> executionMessages = clusterMediator.getAllExecutionMessages();
            for (ExecutionMessage executionMessage : executionMessages) {
                try {
                    if (clusterMediator.isExecutingAction(executionMessage.getExecutionId(),
                        executionMessage.getActionInstanceId())) {
                        invokeListener(executionMessage);
                    }
                } catch (Exception e) {
                    LOGGER.error(String.format("Exception occurred while invoking listener for %s", executionMessage), e);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception occurred in ExecutionStatusChangePollingAgent", e);
        }
    }

    public void invokeListener(ExecutionMessage executionMessage) throws Exception {
        switch (executionMessage.getStatus()) {
            case CANCELED:
                executionStatusChangeListener.onCancel(executionMessage.getExecutionId(), executionMessage.getActionInstanceId());
                break;
            default:
                break;
        }
    }

}

