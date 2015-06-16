package com.netflix.scheduledactions.persistence;

import com.netflix.scheduledactions.Execution;

import java.util.List;

/**
 * @author sthadeshwar
 */
public interface ExecutionDao {

    public String createExecution(String actionInstanceId, Execution execution);

    public void updateExecution(Execution execution);

    public Execution getExecution(String executionId);

    public void deleteExecution(Execution execution);

    public List<Execution> getExecutions(String actionInstanceId, int count);

    public List<Execution> getExecutions(String actionInstanceId);

}
