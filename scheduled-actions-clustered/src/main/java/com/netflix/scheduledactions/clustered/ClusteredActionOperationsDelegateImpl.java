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

import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.scheduledactions.*;
import com.netflix.scheduledactions.clustered.notifications.*;
import com.netflix.scheduledactions.executors.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author sthadeshwar
 */
public class ClusteredActionOperationsDelegateImpl extends AbstractActionOperationsDelegate implements ClusteredActionOperationsDelegate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredActionOperationsDelegateImpl.class);

    private static final long LOCK_TTL = 60L;

    private final ClusterMediator clusterMediator;
    private ActionStatusChangePollingAgent actionStatusChangePollingAgent;
    private ExecutionStatusChangePollingAgent executionStatusChangePollingAgent;

    public ClusteredActionOperationsDelegateImpl(TriggerOperator triggerOperator,
                                                 DaoConfigurer daoConfigurer,
                                                 Executor executor,
                                                 int threadPoolSize,
                                                 ClusterMediator clusterMediator) {
        this(UUID.randomUUID().toString(),
            triggerOperator,
            daoConfigurer,
            executor,
            threadPoolSize,
            clusterMediator);
    }

    public ClusteredActionOperationsDelegateImpl(String delegateId,
                                                 TriggerOperator triggerOperator,
                                                 DaoConfigurer daoConfigurer,
                                                 Executor executor,
                                                 int threadPoolSize,
                                                 ClusterMediator clusterMediator) {
        super(delegateId, triggerOperator, daoConfigurer, executor, threadPoolSize);
        this.clusterMediator = clusterMediator;
        if (clusterMediator == null) {
            throw new IllegalArgumentException("ClusterMediator cannot be null for ClusteredActionOperationsDelegate");
        }
    }

    /*
     * Do NOT add @PostConstruct here
     */
    @Override
    public void initialize() {
        this.actionStatusChangePollingAgent = new ActionStatusChangePollingAgent(
            60*1000L,
            new DefaultActionStatusChangeListener(actionInstanceDao, triggerOperator),
            clusterMediator
        );
        this.executionStatusChangePollingAgent = new ExecutionStatusChangePollingAgent(
            10*1000L,
            new DefaultExecutionStatusChangeListener(
                executionDao,
                actionInstanceDao,
                clusterMediator,
                this),
            clusterMediator
        );
    }

    @Override
    public void destroy() {
        if (this.actionStatusChangePollingAgent != null) {
            this.actionStatusChangePollingAgent.shutdown();
        }
        if (this.executionStatusChangePollingAgent != null) {
            this.executionStatusChangePollingAgent.shutdown();
        }
    }

    @Override
    public boolean isClustered() {
        return true;
    }

    @Override
    public String register(ActionInstance actionInstance) {
        super.register(actionInstance);
        clusterMediator.sendActionMessage(new ActionInstanceMessage(actionInstance.getId(), Status.CREATED));
        return actionInstance.getId();
    }

    @Override
    public void disable(ActionInstance actionInstance) {
        super.disable(actionInstance);
        clusterMediator.sendActionMessage(new ActionInstanceMessage(actionInstance.getId(), Status.DISABLED));
    }

    @Override
    public void enable(ActionInstance actionInstance) {
        super.enable(actionInstance);
        clusterMediator.sendActionMessage(new ActionInstanceMessage(actionInstance.getId(), Status.ENABLED));
    }

    @Override
    public void delete(ActionInstance actionInstance) {
        super.delete(actionInstance);
        clusterMediator.sendActionMessage(new ActionInstanceMessage(actionInstance.getId(), Status.DELETED));
    }

    @Override
    public Execution execute(final ActionInstance actionInstance, String initiator) {
        if (clusterMediator.shouldExecuteAction(actionInstance.getId(), LOCK_TTL)) {
            super.execute(actionInstance, initiator);
        }
        return null;
    }

    @Override
    public void cancel(Execution execution, ActionInstance actionInstance) {
        if (clusterMediator.isExecutingAction(execution.getId(), actionInstance.getId())) {
            super.cancelLocal(execution, actionInstance);
        } else {
            clusterMediator.sendExecutionMessage(
                new ExecutionMessage(execution.getId(), actionInstance.getId(), Status.CANCELED)
            );
        }
    }
}
