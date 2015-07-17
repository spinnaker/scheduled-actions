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
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.scheduledactions.persistence.Codec;
import com.netflix.scheduledactions.persistence.SnappyCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author sthadeshwar
 */
public class ThriftCassandraDao<T> implements CassandraDao<T> {

    public static final String GLOBAL_GROUP = "all";

    private final Class<T> parameterClass;
    private final Keyspace keyspace;
    private final Codec codec;
    private final ObjectMapper objectMapper;
    private final ColumnFamily<String, String> columnFamily;

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper) {
        this.parameterClass = parameterClass;
        this.keyspace = keyspace;
        this.objectMapper = objectMapper;
        this.codec = new SnappyCodec();
        this.columnFamily = ColumnFamily.newColumnFamily(
            toTitleCase(this.parameterClass.getSimpleName()),
            StringSerializer.get(),
            StringSerializer.get(),
            ByteBufferSerializer.get()
        );
    }

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper, Codec codec) {
        this.parameterClass = parameterClass;
        this.keyspace = keyspace;
        this.objectMapper = objectMapper;
        this.codec = codec;
        this.columnFamily = ColumnFamily.newColumnFamily(
            toTitleCase(this.parameterClass.getSimpleName()),
            StringSerializer.get(),
            StringSerializer.get(),
            ByteBufferSerializer.get()
        );
    }

    public ThriftCassandraDao(Class<T> parameterClass, Keyspace keyspace, ObjectMapper objectMapper, Codec codec, String columnFamilyName) {
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
    public void upsert(String id, T value) {
        try {
            byte[] bytes = codec.compress(objectMapper.writeValueAsBytes(value));
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, id).putColumn(id, bytes);
            m.withRow(columnFamily, GLOBAL_GROUP).putColumn(id, new byte[0]);
            m.execute();
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s'", id), e);
        }
    }

    @Override
    public void upsertToGroup(String group, String id, T value) {
        try {
            byte[] bytes = codec.compress(objectMapper.writeValueAsBytes(value));
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, id).putColumn(id, bytes);
            m.withRow(columnFamily, group).putColumn(id, new byte[0]);
            m.execute();
        } catch (ConnectionException | IOException e) {
            throw new RuntimeException(String.format("Exception occurred while upserting value for '%s' and group '%s'", id, group), e);
        }
    }

    @Override
    public void delete(String id) {
        try {
            MutationBatch m = prepareAtomicMutationBatch();
            m.withRow(columnFamily, GLOBAL_GROUP).deleteColumn(id);
            m.withRow(columnFamily, id).deleteColumn(id);
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
            m.withRow(columnFamily, id).deleteColumn(id);
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
            RowQuery<String, String> rowQuery = keyspace.prepareQuery(columnFamily).getKey(group);
            rowQuery.autoPaginate(true).withColumnRange(new RangeBuilder().setLimit(10).setReversed(true).build());
            Collection<String> rowKeys = rowQuery.execute().getResult().getColumnNames();

            // Get values for all the fetched row keys
            RowSliceQuery<String, String> rowSliceQuery = keyspace.prepareQuery(columnFamily).getKeySlice(rowKeys);
            Rows<String, String> rows = rowSliceQuery.execute().getResult();
            for (Row<String, String> row : rows) {
                if (row.getColumns() != null) {
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
        return getGroup(GLOBAL_GROUP);
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
