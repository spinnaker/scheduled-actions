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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author sthadeshwar
 */
public class ActionStatusChangePollingAgent extends AbstractPollingAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionStatusChangePollingAgent.class);

    private final long pollingIntervalMs;
    private final ActionStatusChangeListener actionStatusChangeListener;
    private final ClusterMediator clusterMediator;
    private final Set<ActionInstanceMessage> actionInstanceMessageSet = Collections.emptySet();

    public ActionStatusChangePollingAgent(long pollingIntervalMs,
                                          ActionStatusChangeListener actionStatusChangeListener,
                                          ClusterMediator clusterMediator) {
        this.pollingIntervalMs = pollingIntervalMs;
        this.actionStatusChangeListener = actionStatusChangeListener;
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
            List<ActionInstanceMessage> newList = new ArrayList<>();
            List<ActionInstanceMessage> actionInstanceStatuses = clusterMediator.getAllActionMessages();
            for (ActionInstanceMessage actionInstanceMessage : actionInstanceStatuses) {
                try {
                    if (!actionInstanceMessageSet.contains(actionInstanceMessage)) {
                        newList.add(actionInstanceMessage);
                        invokeListener(actionInstanceMessage);
                    }
                } catch (Exception e) {
                    LOGGER.error(String.format("Exception occurred while invoking listener for %s", actionInstanceMessage), e);
                }
            }
            actionInstanceMessageSet.addAll(newList);
        } catch (Throwable e) {
            LOGGER.error("Exception occurred in PollingTask", e);
        }
    }

    public void invokeListener(ActionInstanceMessage actionInstanceMessage) throws Exception {
        switch (actionInstanceMessage.getStatus()) {
            case CREATED:
                actionStatusChangeListener.onCreate(actionInstanceMessage.getActionInstanceId());
                break;
            case DELETED:
                actionStatusChangeListener.onDelete(actionInstanceMessage.getActionInstanceId());
                break;
            case ENABLED:
                actionStatusChangeListener.onEnable(actionInstanceMessage.getActionInstanceId());
                break;
            case DISABLED:
                actionStatusChangeListener.onDisable(actionInstanceMessage.getActionInstanceId());
                break;
            default:
                break;
        }
    }

}
