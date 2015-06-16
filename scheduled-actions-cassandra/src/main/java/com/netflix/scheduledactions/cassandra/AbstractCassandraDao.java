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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author sthadeshwar
 */
public abstract class AbstractCassandraDao<T> {

    public static final String SEPARATOR = ":";

    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final ObjectMapper objectMapper;
    private final ColumnFamily<String, String> columnFamily;
    private final Class<T> parameterClass;
    private final String columnNameSeparator;

    protected AbstractCassandraDao(Keyspace keyspace, ObjectMapper objectMapper, String columnFamilyName) {
        this.keyspace = keyspace;
        this.objectMapper = objectMapper;
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        this.parameterClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
        this.columnNameSeparator = String.format("%s%s%s", SEPARATOR, parameterClass.getName(), SEPARATOR);
        this.columnFamilyName = columnFamilyName;
        this.columnFamily = ColumnFamily.newColumnFamily(columnFamilyName, StringSerializer.get(), StringSerializer.get(),
            ByteBufferSerializer.get());
    }

    protected AbstractCassandraDao(Keyspace keyspace, ObjectMapper objectMapper) {
        this.keyspace = keyspace;
        this.objectMapper = objectMapper;
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        this.parameterClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
        this.columnNameSeparator = String.format("%s%s%s", SEPARATOR, parameterClass.getName(), SEPARATOR);
        this.columnFamilyName = toTitleCase(this.parameterClass.getSimpleName());
        this.columnFamily = ColumnFamily.newColumnFamily(this.columnFamilyName, StringSerializer.get(), StringSerializer.get(),
            ByteBufferSerializer.get());
    }

    @PostConstruct
    public void init() throws ConnectionException {
        List<String> columnFamilyNames = new ArrayList<>();
        for (ColumnFamilyDefinition cfd : keyspace.describeKeyspace().getColumnFamilyList()) {
            columnFamilyNames.add(cfd.getName());
        }
        if (!columnFamilyNames.contains(columnFamily.getName())) {
            keyspace.createColumnFamily(columnFamily, null);
        }
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    protected void upsert(String rowKey, String columnName, T columnValue) {
        try {
            byte[] bytesValue = objectMapper.writeValueAsBytes(columnValue);
            MutationBatch m = keyspace.prepareMutationBatch();
            m.withRow(columnFamily, rowKey).putColumn(columnName, bytesValue);
            m.execute();
        } catch (ConnectionException | IOException e) {
            throw new CassandraException(
                String.format("Exception occurred while upsert-ing column %s for row %s", columnName, rowKey), e);
        }
    }

    protected void delete(String rowKey, String columnName) {
        try {
            MutationBatch m = keyspace.prepareMutationBatch();
            m.withRow(columnFamily, rowKey).deleteColumn(columnName);
            m.execute();
        } catch (ConnectionException e) {
            throw new CassandraException(
                String.format("Exception occurred while deleting column %s for row %s", columnName, rowKey), e);
        }
    }

    protected Map<String,T> getRow(String rowKey) {
        return getRow(rowKey, null);
    }

    protected Map<String,T> getRow(String rowKey, List<String> columnNames) {
        try {
            RowQuery<String, String> rowQuery = keyspace.prepareQuery(columnFamily).getKey(rowKey);
            if (columnNames != null && columnNames.size() > 0) {
                rowQuery.withColumnSlice(columnNames);
            } else {
                rowQuery.autoPaginate(true).withColumnRange(new RangeBuilder().setLimit(10).setReversed(true).build());
            }
            OperationResult<ColumnList<String>> result;
            Map<String,T> row = new HashMap<>();
            while (!(result = rowQuery.execute()).getResult().isEmpty()) {
                for (Column<String> col : result.getResult()) {
                    row.put(col.getName(), objectMapper.readValue(col.getByteArrayValue(), parameterClass));
                }
            }
            return row;
        } catch (ConnectionException | IOException e) {
            throw new CassandraException(
                String.format("Exception occurred while fetching row %s", rowKey), e);
        }
    }

    protected Map<String, Map<String,T>> getRows() {
        final ConcurrentMap<String, Map<String,T>> rows = new ConcurrentHashMap<>();
        try {
            new AllRowsReader.Builder(keyspace, columnFamily)
                .withPageSize(100) // Read 100 rows at a time
                .withConcurrencyLevel(10) // Split entire token range into 10.  Default is by number of nodes.
                .withPartitioner(null) // this will use keyspace's partitioner
                .forEachRow(new Function<Row<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(Row<String, String> row) {
                        String rowKey = row.getKey();
                        Map<String,T> columns = new HashMap<>();
                        ColumnList<String> columnList = row.getColumns();
                        for (String columnName : columnList.getColumnNames()) {
                            Column<String> column = columnList.getColumnByName(columnName);
                            byte[] bytes = columnList.getColumnByName(columnName).getByteArrayValue();
                            try {
                                columns.put(column.getName(), objectMapper.readValue(bytes, parameterClass));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        rows.put(rowKey, columns);
                        return true;
                    }
                }).build().call();
        } catch (Exception e) {
            throw new CassandraException(
                String.format("Exception occurred while fetching all the rows for column family %s", columnFamilyName), e);
        }
        return rows;
    }

    protected Map<String,T> getColumn(String rowKey, String columnName) throws NotFoundException {
        try {
            Map<String,T> column = new HashMap<>();
            Column<String> col = keyspace.prepareQuery(columnFamily).getKey(rowKey).getColumn(columnName).execute().getResult();
            column.put(col.getName(), objectMapper.readValue(col.getByteArrayValue(), parameterClass));
            return column;
        } catch (NotFoundException nfe) {
            throw nfe;
        } catch (ConnectionException | IOException e) {
            throw new CassandraException(
                String.format("Exception occurred while fetching column %s for row %s", columnName, rowKey), e);
        }
    }

    protected static String toTitleCase(String camelCase) {
        if (camelCase == null || camelCase.length() == 0) return camelCase;
        String regex = "([a-z])([A-Z]+)";
        String replacement = "$1_$2";
        return camelCase.replaceAll(regex, replacement).toLowerCase();
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public Class<T> getParameterClass() {
        return parameterClass;
    }

    protected String createColumnName(String group, String id) {
        if (group == null || id == null || group.contains(columnNameSeparator) || id.contains(columnNameSeparator)) {
            throw new IllegalArgumentException(String.format("Illegal arguments specified for column name creation (group = %s, id = %s)", group, id));
        }
        return String.format("%s%s%s", group, columnNameSeparator, id);
    }

    protected String extractRowKeyFromColumnName(String columnName) {
        if (columnName == null || !columnName.contains(columnNameSeparator)) return columnName;
        String[] tokens = columnName.split(columnNameSeparator);
        if (tokens.length == 2) {
            return tokens[0];
        } else {
            throw new IllegalArgumentException(String.format("Cannot extract row key from column name string: %s", columnName));
        }
    }
}
