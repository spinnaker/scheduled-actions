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

import java.util.List;

public interface RedisDao<T> {

    public void upsert(String id, T value, Integer ttlSeconds);

    public void upsertToGroup(String group, String id, T value, Integer ttlSeconds);

    public void delete(String id);

    public void deleteFromGroup(String group, String id);

    public T get(String id);

    public List<T> getGroup(String group);

    public List<T> getAll();

}
