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

import com.netflix.scheduledactions.Execution;
import com.netflix.scheduledactions.persistence.ExecutionDao;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RedisExecutionDao implements ExecutionDao {

    private final RedisDao<Execution> redisDao;
    private static final int TTL_SECONDS = 60 * 60 * 24;

    public RedisExecutionDao(JedisPool pool) {
        this.redisDao = new JedisDao(Execution.class, pool, new ScheduledActionsObjectMapper());
    }

    @Override
    public String createExecution(String actionInstanceId, Execution execution) {
        execution.setId(UUID.randomUUID().toString());
        redisDao.upsertToGroup(actionInstanceId, execution.getId(), execution, Integer.valueOf(TTL_SECONDS));
        return execution.getId();
    }

    @Override
    public void updateExecution(Execution execution) {
        redisDao.upsert(execution.getId(), execution, Integer.valueOf(TTL_SECONDS));
    }

    @Override
    public Execution getExecution(String executionId) {
        return redisDao.get(executionId);
    }

    @Override
    public void deleteExecution(String actionInstanceId, Execution execution) {
        redisDao.deleteFromGroup(actionInstanceId, execution.getId());
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId, int count) {
        List<Execution> executions = getExecutions(actionInstanceId);
        return executions.size() <= count ? executions : executions.subList(0, count);
    }

    @Override
    public List<Execution> getExecutions(String actionInstanceId) {
        return new ArrayList<>(redisDao.getGroup(actionInstanceId));
    }

}
