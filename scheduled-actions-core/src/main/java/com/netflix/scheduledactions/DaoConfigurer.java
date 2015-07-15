package com.netflix.scheduledactions;

import com.netflix.fenzo.triggers.persistence.TriggerDao;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import com.netflix.scheduledactions.persistence.ExecutionDao;

/**
 * @author sthadeshwar
 */
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
