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

import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class JedisDao<T> implements RedisDao<T> {

    private final ObjectMapper objectMapper;
    private final JedisPool jedisPool;
    private final Class<T> parameterClass;
    private final String prefix;

    public JedisDao(Class<T> parameterClass,
                    JedisPool jedisPool,
                    ObjectMapper objectMapper) {
        this.parameterClass = parameterClass;
        this.jedisPool = jedisPool;
        this.objectMapper = objectMapper;
        this.prefix = toTitleCase(parameterClass.getSimpleName());
    }

    @Override
    public void upsert(String id, T value, Integer ttlSeconds) {
        try (Jedis resource = jedisPool.getResource()) {
            String key = prefix + ":::" + id;
            resource.set(key, objectMapper.writeValueAsString(value));
            resource.expire(key, ttlSeconds);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s'", id), e);
        }
    }

    @Override
    public void upsertToGroup(String group, String id, T value, Integer ttlSeconds) {
        try (Jedis resource = jedisPool.getResource()) {
            String val = objectMapper.writeValueAsString(value);
            String key = prefix + ":::" + group + ":::" + id;
            resource.set(key, val);
            resource.expire(key, ttlSeconds);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s'", id), e);
        }
    }

    @Override
    public void delete(String id) {
        try (Jedis resource = jedisPool.getResource()) {
            String key = prefix + ":::" + id;
            resource.del(key);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while deleting value for '%s'", id), e);
        }
    }

    @Override
    public void deleteFromGroup(String group, String id) {
        try (Jedis resource = jedisPool.getResource()) {
            String key = prefix + ":::" + group + ":::" + id;
            resource.del(key);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while deleting value for '%s' from group '%s'", id, group), e);
        }
    }

    @Override
    public T get(String id) {
        try (Jedis resource = jedisPool.getResource()) {
            String key = prefix + ":::" + id;
            String val = resource.get(key);
            return objectMapper.readValue(val, parameterClass);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while fetching value for '%s'", id), e);
        }
    }

    @Override
    public List<T> getGroup(String group) {
        return getList(prefix + ":::" + group + ":::*");
    }

    @Override
    public List<T> getAll() {
        return getList(prefix + ":::*");
    }

    private List<T> getList(String pattern) {
        try (Jedis resource = jedisPool.getResource()) {
            List<T> vals = new ArrayList<>();
            for (String key : resource.scan(pattern).getResult()) {
                vals.add(objectMapper.readValue(resource.get(key), parameterClass));
            }
            return vals;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Exception occurred while fetching values for group '%s'", pattern), e);
        }

    }

    private static String toTitleCase(String camelCase) {
        if (camelCase == null || camelCase.length() == 0) return camelCase;
        String regex = "([a-z])([A-Z]+)";
        String replacement = "$1_$2";
        return camelCase.replaceAll(regex, replacement).toLowerCase();
    }
}
