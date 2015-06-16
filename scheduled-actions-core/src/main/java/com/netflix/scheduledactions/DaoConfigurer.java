package com.netflix.scheduledactions;

import com.netflix.fenzo.triggers.persistence.TriggerDao;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;

/**
 * @author sthadeshwar
 */
public class DaoConfigurer {
    private final ActionInstanceDao actionInstanceDao;
    private final TriggerDao triggerDao;

    public DaoConfigurer(ActionInstanceDao actionInstanceDao, TriggerDao triggerDao) {
        this.actionInstanceDao = actionInstanceDao;
        this.triggerDao = triggerDao;
    }

    public ActionInstanceDao getActionInstanceDao() {
        return actionInstanceDao;
    }

    public TriggerDao getTriggerDao() {
        return triggerDao;
    }
}
