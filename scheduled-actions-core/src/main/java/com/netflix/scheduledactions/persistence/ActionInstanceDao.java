package com.netflix.scheduledactions.persistence;

import com.netflix.scheduledactions.ActionInstance;

import java.util.List;

/**
 * @author sthadeshwar
 */
public interface ActionInstanceDao {

    public String createActionInstance(String group, ActionInstance actionInstance);
    public void updateActionInstance(ActionInstance actionInstance);
    public ActionInstance getActionInstance(String actionInstanceId);
    public void deleteActionInstance(ActionInstance actionInstance);
    public List<ActionInstance> getActionInstances(String group);
    
}
