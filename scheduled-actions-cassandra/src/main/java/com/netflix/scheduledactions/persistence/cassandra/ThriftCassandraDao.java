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
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.scheduledactions.persistence.Codec;
import com.netflix.scheduledactions.persistence.SnappyCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThriftCassandraDao<T> implements CassandraDao<T> {

    public static final String ALL = "all";

    private final Class<T> parameterClass;
    private final Keyspace keyspace;
    private final Codec codec;
    private final ObjectMapper objectMapper;
    private final ColumnFamily<String, String> columnFamily;

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper) {
        this(parameterClass, keyspace, objectMapper, new SnappyCodec(), toTitleCase(parameterClass.getSimpleName()));
    }

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper, Codec codec) {
        this(parameterClass, keyspace, objectMapper, codec, toTitleCase(parameterClass.getSimpleName()));
    }

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper, String columnFamilyName) {
        this(parameterClass, keyspace, objectMapper, new SnappyCodec(), columnFamilyName);
    }

    public ThriftCassandraDao(Class<T> parameterClass,
                              Keyspace keyspace,
                              ObjectMapper objectMapper,
                              Codec codec,
                              String columnFamilyName) {
        this.parameterClass = parameterClass;
        this.keyspace = keyspace;
        this.objectMapper = objectMapper;
        this.codec = codec;
        this.columnFamily = ColumnFamily.newColumnFamily(
            columnFamilyName,
            StringSerializer.get(),
            StringSerializer.get(),
            ByteBufferSerializer.get()
        );
    }

    @Override
    public void createColumnFamily() {
        try {
            List<String> columnFamilyNames = new ArrayList<>();
            for (ColumnFamilyDefinition cfd : keyspace.describeKeyspace().getColumnFamilyList()) {
                columnFamilyNames.add(cfd.getName());
            }
            if (!columnFamilyNames.contains(columnFamily.getName())) {
                keyspace.createColumnFamily(columnFamily, null);
            }
        } catch (ConnectionException e) {
            throw new RuntimeException(String.format("Exception occurred while creating column family '%s'", columnFamily.getName()), e);
        }
    }

    @Override
    public void upsert(String id, T value, Integer ttlSeconds) {
        try {
            byte[] bytes = codec.compress(objectMapper.writeValueAsBytes(value));
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, id).putColumn(id, bytes, ttlSeconds);
            m.withRow(columnFamily, ALL).putColumn(id, new byte[0], ttlSeconds);
            m.execute();
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s'", id), e);
        }
    }

    @Override
    public void upsertToGroup(String group, String id, T value, Integer ttlSeconds) {
        try {
            byte[] bytes = codec.compress(objectMapper.writeValueAsBytes(value));
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, id).putColumn(id, bytes, ttlSeconds);
            m.withRow(columnFamily, group).putColumn(id, new byte[0], ttlSeconds);
            m.withRow(columnFamily, ALL).putColumn(id, new byte[0], ttlSeconds);
            m.execute();
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s' and group '%s'", id, group), e);
        }
    }

    @Override
    public void delete(String id) {
        try {
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, ALL).deleteColumn(id);
            m.withRow(columnFamily, id).delete();
            m.execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(String.format("Exception occurred while deleting value for '%s'", id), e);
        }
    }

    @Override
    public void deleteFromGroup(String group, String id) {
        try {
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, group).deleteColumn(id);
            m.withRow(columnFamily, ALL).deleteColumn(id);
            m.withRow(columnFamily, id).delete();
            m.execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(String.format("Exception occurred while deleting value for '%s' from group '%s'", id, group), e);
        }
    }

    @Override
    public T get(String id) {
        try {
            Column<String> col = keyspace.prepareQuery(columnFamily).getKey(id).getColumn(id).execute().getResult();
            byte[] bytes = codec.decompress(col.getByteArrayValue());
            return objectMapper.readValue(bytes, parameterClass);
        } catch (NotFoundException e) {
            return null;
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while fetching value for '%s'", id), e);
        }
    }

    @Override
    public List<T> getGroup(String group) {
        try {
            List<T> list = new ArrayList<>();

            // Get all the row keys
            RowQuery<String, String> rowQuery = keyspace.prepareQuery(columnFamily).getKey(group)
                .autoPaginate(true)
                .withColumnRange(new RangeBuilder().setLimit(1000).build());

            List<String> rowKeys = new ArrayList<>();
            ColumnList<String> columns;
            while (!(columns = rowQuery.execute().getResult()).isEmpty()) {
                for (Column<String> c : columns) {
                    rowKeys.add(c.getName());
                }
            }

            // Get values for all the fetched row keys
            RowSliceQuery<String, String> rowSliceQuery = keyspace.prepareQuery(columnFamily).getKeySlice(rowKeys);
            Rows<String, String> rows = rowSliceQuery.execute().getResult();
            for (Row<String, String> row : rows) {
                if (row.getColumns() != null && row.getColumns().size() > 0) {
                    byte[] bytes = row.getColumns().getColumnByIndex(0).getByteArrayValue();
                    list.add(objectMapper.readValue(codec.decompress(bytes), parameterClass));
                }
            }

            return list;
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while fetching values for group '%s'", group), e);
        }
    }

    @Override
    public List<T> getAll() {
        return getGroup(ALL);
    }

    private MutationBatch prepareAtomicMutationBatch() {
        return keyspace.prepareMutationBatch().withAtomicBatch(true);
    }

    private static String toTitleCase(String camelCase) {
        if (camelCase == null || camelCase.length() == 0) return camelCase;
        String regex = "([a-z])([A-Z]+)";
        String replacement = "$1_$2";
        return camelCase.replaceAll(regex, replacement).toLowerCase();
    }
}
