package com.netflix.scheduledactions.persistence;

import com.netflix.scheduledactions.ActionInstance;

import java.util.List;
import java.util.UUID;

/**
 * @author sthadeshwar
 */
public class InMemoryActionInstanceDao extends AbstractInMemoryDao<ActionInstance> implements ActionInstanceDao {

    @Override
    public String createActionInstance(String group, ActionInstance actionInstance) {
        if (actionInstance.getId() == null) {
            actionInstance.setId(createId(group, UUID.randomUUID().toString()));
        }
        create(group, actionInstance.getId(), actionInstance);
        return actionInstance.getId();
    }

    @Override
    public void updateActionInstance(ActionInstance actionInstance) {
        String group = extractGroupFromId(actionInstance.getId());
        update(group, actionInstance.getId(), actionInstance);
    }

    @Override
    public ActionInstance getActionInstance(String actionInstanceId) {
        String group = extractGroupFromId(actionInstanceId);
        return read(group, actionInstanceId);
    }

    @Override
    public void deleteActionInstance(String group, ActionInstance actionInstance) {
        delete(group, actionInstance.getId());
    }

    @Override
    public List<ActionInstance> getActionInstances(String group) {
        return list(group);
    }

    @Override
    public List<ActionInstance> getActionInstances() {
        return list();
    }
}
