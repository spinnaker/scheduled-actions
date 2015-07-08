package com.netflix.scheduledactions.persistence;

import com.netflix.scheduledactions.Execution;
import com.netflix.fenzo.triggers.persistence.AbstractInMemoryDao;

import java.util.List;
import java.util.UUID;

/**
 * @author sthadeshwar
 */
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
