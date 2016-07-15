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

package com.netflix.scheduledactions.persistence.redis;

import com.netflix.scheduledactions.ActionInstance;
import com.netflix.scheduledactions.persistence.ActionInstanceDao;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RedisActionInstanceDao implements ActionInstanceDao {

    private final RedisDao<ActionInstance> redisDao;

    public RedisActionInstanceDao(JedisPool pool) {
        this.redisDao = new JedisDao(ActionInstance.class, pool, new ScheduledActionsObjectMapper());
    }

    @Override
    public String createActionInstance(String group, ActionInstance actionInstance) {
        if (actionInstance.getId() == null) {
            actionInstance.setId(UUID.randomUUID().toString());
        }
        redisDao.upsertToGroup(group, actionInstance.getId(), actionInstance, 0);
        return actionInstance.getId();
    }

    @Override
    public void updateActionInstance(ActionInstance actionInstance) {
        redisDao.upsert(actionInstance.getId(), actionInstance, 0);
    }

    @Override
    public ActionInstance getActionInstance(String actionInstanceId) {
        return redisDao.get(actionInstanceId);
    }

    @Override
    public void deleteActionInstance(String group, ActionInstance actionInstance) {
        redisDao.deleteFromGroup(group, actionInstance.getId());
    }

    @Override
    public List<ActionInstance> getActionInstances(String group) {
        return new ArrayList(redisDao.getGroup(group));
    }

    @Override
    public List<ActionInstance> getActionInstances() {
        return new ArrayList(redisDao.getAll());
    }
}
