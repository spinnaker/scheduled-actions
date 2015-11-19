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

import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.ClusteredActionOperationsDelegate;
import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.clustered.ClusterMediator;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.scheduledactions.persistence.ExecutionDao;

public class DefaultExecutionStatusChangeListener implements ExecutionStatusChangeListener {

    private final ExecutionDao executionDao;
    private final ActionInstanceDao actionInstanceDao;
    private final ClusterMediator clusterMediator;
    private final ClusteredActionOperationsDelegate actionOperationsDelegate;

    public DefaultExecutionStatusChangeListener(ExecutionDao executionDao,
                                                ActionInstanceDao actionInstanceDao,
                                                ClusterMediator clusterMediator,
                                                ClusteredActionOperationsDelegate actionOperationsDelegate) {
        this.executionDao = executionDao;
        this.actionInstanceDao = actionInstanceDao;
        this.clusterMediator = clusterMediator;
        this.actionOperationsDelegate = actionOperationsDelegate;
    }

    @Override
    public void onCancel(String executionId, String actionInstanceId) {
        if (clusterMediator.isExecutingAction(executionId, actionInstanceId)) {
            Execution execution = executionDao.getExecution(executionId);
            if (execution != null) {
                ActionInstance actionInstance = actionInstanceDao.getActionInstance(execution.getActionInstanceId());
                if (actionInstance != null) {
                    actionOperationsDelegate.cancel(execution, actionInstance);
                }
            }
        }
    }

}
