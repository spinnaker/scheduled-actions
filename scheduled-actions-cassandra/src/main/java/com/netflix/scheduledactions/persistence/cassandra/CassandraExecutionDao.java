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

package com.netflix.scheduledactions.persistence.cassandra;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.persistence.ExecutionDao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author sthadeshwar
 */
public class CassandraExecutionDao extends AbstractCassandraDao<Execution> implements ExecutionDao {

    public CassandraExecutionDao(Keyspace keyspace) {
        super(keyspace, new ScheduledActionsObjectMapper());
    }

    @Override
    public String createExecution(String actionInstanceId, Execution execution) {
        execution.setId(createColumnName(actionInstanceId, UUID.randomUUID().toString()));
        upsert(actionInstanceId, execution.getId(), execution);
        return execution.getId();
    }

    @Override
    public void updateExecution(Execution execution) {
        String actionInstanceId = extractRowKeyFromColumnName(execution.getId());
        upsert(actionInstanceId, execution.getId(), execution);
    }

    @Override
    public Execution getExecution(String executionId) {
        String actionInstanceId = extractRowKeyFromColumnName(executionId);
        try {
            Map<String, Execution> executionMap = getColumn(actionInstanceId, executionId);
            return executionMap.get(executionId);
        } catch (NotFoundException nfe) {
            return null;
        }
    }

    @Override
    public void deleteExecution(Execution execution) {
        String actionInstanceId = extractRowKeyFromColumnName(execution.getId());
        delete(actionInstanceId, execution.getId());
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId, int count) {
        List<Execution> executions = getExecutions(actionInstanceId);
        return executions.size() <= count ? executions : executions.subList(0, count);
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId) {
      return new ArrayList<>(getRow(actionInstanceId).values());
    }

}
