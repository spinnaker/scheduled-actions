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

package com.netflix.scheduledactions.cassandra;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author sthadeshwar
 */
public class CassandraActionInstanceDao extends AbstractCassandraDao<ActionInstance> implements ActionInstanceDao {

    public CassandraActionInstanceDao(Keyspace keyspace) {
        super(keyspace, new ScheduledActionsObjectMapper());
    }

    @Override
    public String createActionInstance(String group, ActionInstance actionInstance) {
        actionInstance.setId(createColumnName(group, UUID.randomUUID().toString()));
        upsert(group, actionInstance.getId(), actionInstance);
        return actionInstance.getId();
    }

    @Override
    public void updateActionInstance(ActionInstance actionInstance) {
        String group = extractRowKeyFromColumnName(actionInstance.getId());
        upsert(group, actionInstance.getId(), actionInstance);
    }

    @Override
    public ActionInstance getActionInstance(String actionInstanceId) {
        String group = extractRowKeyFromColumnName(actionInstanceId);
        try {
            Map<String, ActionInstance> executionMap = getColumn(group, actionInstanceId);
            return executionMap.get(actionInstanceId);
        } catch (NotFoundException nfe) {
            return null;
        }
    }

    @Override
    public void deleteActionInstance(ActionInstance actionInstance) {
        String group = extractRowKeyFromColumnName(actionInstance.getId());
        delete(group, actionInstance.getId());
    }

    @Override
    public List<ActionInstance> getActionInstances(String group) {
        return new ArrayList<>(getRow(group).values());
    }
}
