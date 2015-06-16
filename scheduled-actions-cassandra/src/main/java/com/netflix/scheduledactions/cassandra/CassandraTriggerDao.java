package com.netflix.scheduledactions.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.fenzo.triggers.Trigger;
import com.netflix.fenzo.triggers.persistence.TriggerDao;

import java.util.*;

/**
 * @author sthadeshwar
 */
public class CassandraTriggerDao extends AbstractCassandraDao<Trigger> implements TriggerDao {

    public CassandraTriggerDao(Keyspace keyspace, ObjectMapper objectMapper) {
        super(keyspace, objectMapper);
    }

    @Override
    public String createTrigger(String triggerGroup, Trigger trigger) {
        trigger.setId(createColumnName(triggerGroup, UUID.randomUUID().toString()));
        upsert(triggerGroup, trigger.getId(), trigger);
        return trigger.getId();
    }

    @Override
    public void updateTrigger(Trigger trigger) {
        String triggerGroup = extractRowKeyFromColumnName(trigger.getId());
        upsert(triggerGroup, trigger.getId(), trigger);
    }

    @Override
    public Trigger getTrigger(String triggerId) {
        String triggerGroup = extractRowKeyFromColumnName(triggerId);
        try {
            Map<String, Trigger> triggerMap = getColumn(triggerGroup, triggerId);
            Trigger trigger = triggerMap.get(triggerId);
            updateActualTriggerType(trigger);
            return trigger;
        } catch (NotFoundException nfe) {
            return null;
        }
    }

    @Override
    public void deleteTrigger(Trigger trigger) {
        String triggerGroup = extractRowKeyFromColumnName(trigger.getId());
        delete(triggerGroup, trigger.getId());
    }

    @Override
    public List<Trigger> getTriggers(String triggerGroup) {
        List<Trigger> triggers = new ArrayList(getRow(triggerGroup).values());
        updateActualTriggerTypes(triggers);
        return triggers;
    }

    @Override
    public List<Trigger> getTriggers() {
        List<Trigger> allTriggers = new ArrayList<>();
        Map<String, Map<String,Trigger>> rows = getRows();
        for (Iterator<String> iterator = rows.keySet().iterator(); iterator.hasNext();) {
            Map<String,Trigger> triggers = rows.get(iterator.next());
            allTriggers.addAll(triggers.values());
        }
        updateActualTriggerTypes(allTriggers);
        return allTriggers;
    }

    private void updateActualTriggerType(Trigger trigger) {
        try {
            ObjectMapper objectMapper = getObjectMapper();
            String data = objectMapper.writeValueAsString(trigger.getData());
            trigger.setData(getObjectMapper().readValue(data, trigger.getDataType()));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while updating actual trigger type for trigger %s", trigger), e);
        }
    }

    private void updateActualTriggerTypes(List<Trigger> triggers) {
        for(Trigger trigger : triggers) {
            updateActualTriggerType(trigger);
        }
    }

}
