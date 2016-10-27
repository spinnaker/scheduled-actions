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

import com.netflix.scheduledactions.ActionInstance;

import java.util.List;
import java.util.UUID;

public class InMemoryActionInstanceDao extends AbstractInMemoryDao<ActionInstance> implements ActionInstanceDao {

    @Override
    public String createActionInstance(String group, ActionInstance actionInstance) {
        if (actionInstance.getId() == null) {
            actionInstance.setId(createId(group, UUID.randomUUID().toString()));
        } else if (!isIdFormat(actionInstance.getId())) {
            actionInstance.setId(createId(group, actionInstance.getId()));
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
