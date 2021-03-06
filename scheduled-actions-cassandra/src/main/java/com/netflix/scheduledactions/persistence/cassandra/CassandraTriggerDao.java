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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.Keyspace;
import com.netflix.fenzo.triggers.Trigger;
import com.netflix.fenzo.triggers.persistence.TriggerDao;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;

public class CassandraTriggerDao implements TriggerDao {

    private final CassandraDao<Trigger> cassandraDao;
    private final ObjectMapper objectMapper;

    public CassandraTriggerDao(Keyspace keyspace) {
        this.objectMapper = new ScheduledActionsObjectMapper();
        this.cassandraDao = new ThriftCassandraDao(Trigger.class, keyspace, objectMapper);
    }

    @PostConstruct
    public void init() {
        this.cassandraDao.createColumnFamily();
    }

    @Override
    public String createTrigger(String triggerGroup, Trigger trigger) {
        trigger.setId(UUID.randomUUID().toString());
        cassandraDao.upsertToGroup(triggerGroup, trigger.getId(), trigger, null);
        return trigger.getId();
    }

    @Override
    public void updateTrigger(Trigger trigger) {
        cassandraDao.upsert(trigger.getId(), trigger, null);
    }

    @Override
    public Trigger getTrigger(String triggerId) {
        Trigger trigger = cassandraDao.get(triggerId);
        updateActualTriggerType(trigger);
        return trigger;
    }

    @Override
    public void deleteTrigger(String triggerGroup, Trigger trigger) {
        cassandraDao.deleteFromGroup(triggerGroup, trigger.getId());
    }

    @Override
    public List<Trigger> getTriggers(String triggerGroup) {
        List<Trigger> triggers = cassandraDao.getGroup(triggerGroup);
        updateActualTriggerTypes(triggers);
        return triggers;
    }

    @Override
    public List<Trigger> getTriggers() {
        List<Trigger> triggers = cassandraDao.getAll();
        updateActualTriggerTypes(triggers);
        return triggers;
    }

    private void updateActualTriggerType(Trigger trigger) {
        try {
            String data = objectMapper.writeValueAsString(trigger.getData());
            trigger.setData(objectMapper.readValue(data, trigger.getDataType()));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while updating actual trigger type for trigger %s", trigger), e);
        }
    }

    private void updateActualTriggerTypes(List<Trigger> triggers) {
        for (Trigger trigger : triggers) {
            updateActualTriggerType(trigger);
        }
    }

}