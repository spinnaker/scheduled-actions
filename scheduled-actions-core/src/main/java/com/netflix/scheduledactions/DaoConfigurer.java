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

package com.netflix.scheduledactions;

import com.netflix.fenzo.triggers.persistence.TriggerDao;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.scheduledactions.persistence.ExecutionDao;

public class DaoConfigurer {
    private final ActionInstanceDao actionInstanceDao;
    private final TriggerDao triggerDao;
    private final ExecutionDao executionDao;

    public DaoConfigurer(ActionInstanceDao actionInstanceDao, TriggerDao triggerDao, ExecutionDao executionDao) {
        this.actionInstanceDao = actionInstanceDao;
        this.triggerDao = triggerDao;
        this.executionDao = executionDao;
    }

    public ActionInstanceDao getActionInstanceDao() {
        return actionInstanceDao;
    }

    public TriggerDao getTriggerDao() {
        return triggerDao;
    }

    public ExecutionDao getExecutionDao() {
        return executionDao;
    }
}
