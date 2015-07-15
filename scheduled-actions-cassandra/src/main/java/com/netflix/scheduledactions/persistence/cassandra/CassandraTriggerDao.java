package com.netflix.scheduledactions.persistence.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.Keyspace;
import com.netflix.fenzo.triggers.Trigger;
import com.netflix.fenzo.triggers.persistence.TriggerDao;

import java.util.List;
import java.util.UUID;

/**
 * @author sthadeshwar
 */
public class CassandraTriggerDao implements TriggerDao {

    private final CassandraDao<Trigger> cassandraDao;
    private final ObjectMapper objectMapper;

    public CassandraTriggerDao(Keyspace keyspace) {
        this.objectMapper = new ScheduledActionsObjectMapper();
        this.cassandraDao = new ThriftCassandraDao(Trigger.class, keyspace, objectMapper);
    }

    @Override
    public String createTrigger(String triggerGroup, Trigger trigger) {
        trigger.setId(UUID.randomUUID().toString());
        cassandraDao.upsertToGroup(triggerGroup, trigger.getId(), trigger);
        return trigger.getId();
    }

    @Override
    public void updateTrigger(Trigger trigger) {
        cassandraDao.upsert(trigger.getId(), trigger);
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