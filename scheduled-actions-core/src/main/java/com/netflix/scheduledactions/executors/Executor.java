package com.netflix.scheduledactions.executors;

import com.netflix.scheduledactions.Action;
import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.exceptions.ExecutionException;
import com.netflix.scheduledactions.persistence.ExecutionDao;

/**
 * @author sthadeshwar
 */
public interface Executor {
    public ExecutionDao getExecutionDao();
    public void execute(Action action, ActionInstance actionInstance, Execution execution) throws ExecutionException;
    public void cancel(Action action, ActionInstance actionInstance, Execution execution) throws ExecutionException;
}
