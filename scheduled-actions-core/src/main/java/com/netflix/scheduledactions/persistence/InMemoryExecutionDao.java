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

package com.netflix.scheduledactions.persistence;

import com.netflix.scheduledactions.Execution;
import com.netflix.fenzo.triggers.persistence.AbstractInMemoryDao;

import java.util.List;
import java.util.UUID;

public class InMemoryExecutionDao extends AbstractInMemoryDao<Execution> implements ExecutionDao {

    @Override
    public String createExecution(String actionInstanceId, Execution execution) {
        execution.setId(createId(actionInstanceId, UUID.randomUUID().toString()));
        create(actionInstanceId, execution.getId(), execution);
        return execution.getId();
    }

    @Override
    public void updateExecution(Execution execution) {
        try {
            String actionInstanceId = extractGroupFromId(execution.getId());
            update(actionInstanceId, execution.getId(), execution);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Execution getExecution(String executionId) {
        String actionInstanceId = extractGroupFromId(executionId);
        return read(actionInstanceId, executionId);
    }

    @Override
    public void deleteExecution(String actionInstanceId, Execution execution) {
        delete(actionInstanceId, execution.getId());
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId, int count) {
        return list(actionInstanceId, count);
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId) {
        return list(actionInstanceId);
    }

}
