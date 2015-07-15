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

import com.netflix.fenzo.triggers.ScheduledTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sthadeshwar
 */
public class DefaultActionStatusChangeListener implements ActionStatusChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultActionStatusChangeListener.class);

    private final ActionInstanceDao actionInstanceDao;
    private final TriggerOperator triggerOperator;

    public DefaultActionStatusChangeListener(ActionInstanceDao actionInstanceDao, TriggerOperator triggerOperator) {
        this.actionInstanceDao = actionInstanceDao;
        this.triggerOperator = triggerOperator;
    }

    @Override
    public void onCreate(String actionInstanceId) throws SchedulerException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (!isScheduled(actionInstance)) {
            triggerOperator.scheduleTrigger((ScheduledTrigger) actionInstance.getFenzoTrigger());
        }
    }

    @Override
    public void onDisable(String actionInstanceId) throws SchedulerException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (isScheduled(actionInstance)) {
            triggerOperator.disableTrigger(actionInstance.getFenzoTrigger());
        }
    }

    @Override
    public void onEnable(String actionInstanceId) throws SchedulerException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (!isScheduled(actionInstance)) {
            triggerOperator.enableTrigger(actionInstance.getFenzoTrigger());
        }
    }

    @Override
    public void onDelete(String actionInstanceId) throws SchedulerException {
        ActionInstance actionInstance = actionInstanceDao.getActionInstance(actionInstanceId);
        if (isScheduled(actionInstance)) {
            triggerOperator.unscheduleTrigger((ScheduledTrigger) actionInstance.getFenzoTrigger());
        }
    }

    private boolean isScheduled(ActionInstance actionInstance) {
        try {
            if (actionInstance != null &&
                actionInstance.getFenzoTrigger() != null &&
                actionInstance.getFenzoTrigger() instanceof ScheduledTrigger) {
                return triggerOperator.isScheduled((ScheduledTrigger) actionInstance.getFenzoTrigger());
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurred while checking actionInstance {}", actionInstance, e);
        }
        return false;
    }

}
