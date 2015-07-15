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
import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author sthadeshwar
 */
public class CassandraActionInstanceDao implements ActionInstanceDao {

    private final CassandraDao<ActionInstance> cassandraDao;

    public CassandraActionInstanceDao(Keyspace keyspace) {
        this.cassandraDao = new ThriftCassandraDao(ActionInstance.class, keyspace, new ScheduledActionsObjectMapper());
    }

    @Override
    public String createActionInstance(String group, ActionInstance actionInstance) {
        if (actionInstance.getId() == null) {
            actionInstance.setId(UUID.randomUUID().toString());
        }
        cassandraDao.upsertToGroup(group, actionInstance.getId(), actionInstance);
        return actionInstance.getId();
    }

    @Override
    public void updateActionInstance(ActionInstance actionInstance) {
        cassandraDao.upsert(actionInstance.getId(), actionInstance);
    }

    @Override
    public ActionInstance getActionInstance(String actionInstanceId) {
        return cassandraDao.get(actionInstanceId);
    }

    @Override
    public void deleteActionInstance(String group, ActionInstance actionInstance) {
        cassandraDao.deleteFromGroup(group, actionInstance.getId());
    }

    @Override
    public List<ActionInstance> getActionInstances(String group) {
        return new ArrayList(cassandraDao.getGroup(group));
    }

    @Override
    public List<ActionInstance> getActionInstances() {
        return new ArrayList(cassandraDao.getAll());
    }
}
