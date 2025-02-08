package org.influxdb.impl;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.*;

import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class InfluxDBParallelImpl implements InfluxDB{
    private final Map<List<String>, InfluxDB> influxDBMap = new HashMap<>();

    public InfluxDBParallelImpl(List<InfluxDB> influxDBConnections, List<String> databases){
        for (int i = 0; i < influxDBConnections.size(); i++) {
            influxDBConnections.get(i).setDatabase(databases.get(i));
            QueryResult result = influxDBConnections.get(i).query(new Query("SHOW MEASUREMENTS"));
            List<String> measurements = extractMeasurements(result);
            influxDBMap.put(measurements,influxDBConnections.get(i));
        }
    }

    private List<String> extractMeasurements(QueryResult queryResult){
        List<String> measurements = new ArrayList<>();
        if (queryResult.getResults()!=null) {
            for(QueryResult.Result result : queryResult.getResults()){
                if (result!=null && result.getSeries()!=null) {
                    for (QueryResult.Series series:result.getSeries()){
                        if (series != null && series.getValues() != null) {
                            for (List<Object> valueRow : series.getValues()){
                                if (valueRow!=null && !valueRow.isEmpty()) {
                                    measurements.add(String.valueOf(valueRow.get(0)));
                                }
                            }
                        }
                    }
                }
            }
        }
        return measurements;
    }

    private InfluxDB getCorrespondingConnection(String measurement){
        return  influxDBMap.entrySet()
                .stream()
                .filter(entry -> entry.getKey().contains(measurement.replaceAll("\"","")))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private List<String> extractUniqueMeasurements(String records) {
        String[] lines = records.split("\n");

        Set<String> measurements = new HashSet<>();

        for (String line : lines) {
            if (line.contains(",")) {
                String measurement = line.substring(0, line.indexOf(","));
                measurements.add(measurement);
            }
        }

        return new ArrayList<>(measurements);
    }
    private String extractMeasurementFromQuery(String query) {
        query = query.trim().toUpperCase();
        int fromIndex = query.indexOf("FROM");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid Influx query: 'FROM' keyword not found.");
        }
        String afterFrom = query.substring(fromIndex + 4).trim();
        String[] tokens = afterFrom.split("\\s+");
        return tokens.length > 0 ? tokens[0] : null;
    }

    @Override
    public InfluxDB setLogLevel(LogLevel logLevel) {
        influxDBMap.values()
                .forEach(con->con.setLogLevel(logLevel));
        return this;
    }

    @Override
    public InfluxDB enableGzip() {
        influxDBMap.values()
                .forEach(InfluxDB::enableGzip);
        return this;
    }

    @Override
    public InfluxDB disableGzip() {
        influxDBMap.values()
                .forEach(InfluxDB::disableGzip);
        return this;
    }

    @Override
    public boolean isGzipEnabled() {
        List<Boolean> isGzipEnabledForAll = influxDBMap.values().stream()
                .map(InfluxDB::isGzipEnabled).collect(Collectors.toList());
        return !isGzipEnabledForAll.contains(false);
    }

    @Override
    public InfluxDB enableBatch() {
        enableBatch(BatchOptions.DEFAULTS);
        return this;
    }

    @Override
    public InfluxDB enableBatch(BatchOptions batchOptions) {
        influxDBMap.values()
                .forEach(influxDB->influxDB.enableBatch(batchOptions));
        return this;
    }

    @Override
    public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.enableBatch(actions,flushDuration,flushDurationTimeUnit));
        return this;
    }

    @Override
    public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit, ThreadFactory threadFactory) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.enableBatch(actions,flushDuration,flushDurationTimeUnit,threadFactory));
        return this;
    }

    @Override
    public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit, ThreadFactory threadFactory, BiConsumer<Iterable<Point>, Throwable> exceptionHandler, ConsistencyLevel consistency) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.enableBatch(actions,
                        flushDuration,
                        flushDurationTimeUnit,
                        threadFactory,
                        exceptionHandler,
                        consistency));
        return this;
    }

    @Override
    public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit, ThreadFactory threadFactory, BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
        influxDBMap.values()
                .forEach(influxDB ->influxDB.enableBatch(actions,
                        flushDuration,
                        flushDurationTimeUnit,
                        threadFactory,
                        exceptionHandler
                        ));
        return this;
    }

    @Override
    public void disableBatch() {
        influxDBMap.values()
                .forEach(InfluxDB::disableBatch);
    }

    @Override
    public boolean isBatchEnabled() {
        List<Boolean> isBatchEnabled = influxDBMap.values()
                .stream().map(InfluxDB::isBatchEnabled)
                .collect(Collectors.toList());
        return !isBatchEnabled.contains(false);
    }

    @Override
    public Pong ping() {
        List<Pong> pongs = influxDBMap.values()
                .stream().map(InfluxDB::ping)
                .collect(Collectors.toList());
        return pongs.isEmpty()?null: pongs.get(0);
    }

    @Override
    public List<Pong> pings(){
        return influxDBMap.values()
                .stream().map(InfluxDB::ping)
                .collect(Collectors.toList());
    }

    @Override
    public String version() {
        List<String> versions = influxDBMap.values()
                .stream().map(InfluxDB::version)
                .collect(Collectors.toList());
        return versions.isEmpty()?null:versions.get(0);
    }

    @Override
    public List<String> versions() {
        return influxDBMap.values()
                .stream().map(InfluxDB::version)
                .collect(Collectors.toList());
    }


    @Override
    public void write(Point point) {
        String measurement = point.getMeasurement();
        if (measurement!=null) {
            InfluxDB influxDB = getCorrespondingConnection(measurement);
            if (influxDB!=null) {
                influxDB.write(point);
            }
        }
    }

    @Override
    public void write(String records) {
        List<String> measurements = extractUniqueMeasurements(records);
        if (!measurements.isEmpty()) {
            InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
            if (influxDB!=null) {
                influxDB.write(records);
            }
        }
    }

    @Override
    public void write(List<String> records) {
        if (!records.isEmpty()) {
            List<String> measurements = extractUniqueMeasurements(records.get(0));
            if (!measurements.isEmpty()) {
                InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
                if (influxDB!=null) {
                    influxDB.write(records);
                }
            }
        }
    }

    @Override
    public void write(String database, String retentionPolicy, Point point) {
        String measurement = point.getMeasurement();
        if (measurement!=null) {
            InfluxDB influxDB = getCorrespondingConnection(measurement);
            if (influxDB!=null) {
                influxDB.write(database,retentionPolicy,point);
            }
        }
    }

    @Override
    public void write(int udpPort, Point point) {
        String measurement = point.getMeasurement();
        if (measurement!=null) {
            InfluxDB influxDB = getCorrespondingConnection(measurement);
            if (influxDB!=null) {
                influxDB.write(udpPort,point);
            }
        }
    }

    @Override
    public void write(BatchPoints batchPoints) {
        if(!batchPoints.getPoints().isEmpty()) {
            String measurement = batchPoints.getPoints().get(0).getMeasurement();
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                if (influxDB!=null) {
                    influxDB.write(batchPoints);
                }
            }
        }

    }

    @Override
    public void writeWithRetry(BatchPoints batchPoints) {
        if(!batchPoints.getPoints().isEmpty()) {
            String measurement = batchPoints.getPoints().get(0).getMeasurement();
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                if (influxDB!=null) {
                    influxDB.writeWithRetry(batchPoints);
                }
            }
        }
    }

    @Override
    public void write(String database, String retentionPolicy, ConsistencyLevel consistency, String records) {
        List<String> measurements = extractUniqueMeasurements(records);
        if (!measurements.isEmpty()) {
            InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
            if (influxDB!=null) {
                influxDB.write(database,retentionPolicy,consistency,records);
            }
        }
    }

    @Override
    public void write(String database, String retentionPolicy, ConsistencyLevel consistency, TimeUnit precision, String records) {
        List<String> measurements = extractUniqueMeasurements(records);
        if (!measurements.isEmpty()) {
            InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
            if (influxDB!=null) {
                influxDB.write(database,retentionPolicy,consistency,precision,records);
            }
        }
    }

    @Override
    public void write(String database, String retentionPolicy, ConsistencyLevel consistency, List<String> records) {
        if (!records.isEmpty()) {
            List<String> measurements = extractUniqueMeasurements(records.get(0));
            if (!measurements.isEmpty()) {
                InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
                if (influxDB!=null) {
                    influxDB.write(database,retentionPolicy,consistency,records);
                }
            }
        }
    }

    @Override
    public void write(String database, String retentionPolicy, ConsistencyLevel consistency, TimeUnit precision, List<String> records) {
        if (!records.isEmpty()) {
            List<String> measurements = extractUniqueMeasurements(records.get(0));
            if (!measurements.isEmpty()) {
                InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
                if (influxDB!=null) {
                    influxDB.write(database,retentionPolicy,consistency,precision,records);
                }
            }
        }
    }

    @Override
    public void write(int udpPort, String records) {
        List<String> measurements = extractUniqueMeasurements(records);
        if (!measurements.isEmpty()) {
            InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
            if (influxDB!=null) {
                influxDB.write(udpPort,records);
            }
        }
    }

    @Override
    public void write(int udpPort, List<String> records) {
        if (!records.isEmpty()) {
            List<String> measurements = extractUniqueMeasurements(records.get(0));
            if (!measurements.isEmpty()) {
                InfluxDB influxDB = getCorrespondingConnection(measurements.get(0));
                if (influxDB!=null) {
                    influxDB.write(udpPort,records);
                }
            }
        }
    }

    @Override
    public QueryResult query(Query query) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                return influxDB.query(query);
            }
            return null;
        }else {
            InfluxDB influxDB = new ArrayList<>(influxDBMap.values()).get(0);
            return influxDB.query(query);
        }
    }
    @Override
    public List<QueryResult> queries(Query query) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                return Collections.singletonList(influxDB.query(query));
            }
            return null;
        }else {
            List<QueryResult> queryResults = new ArrayList<>();
            influxDBMap.values()
                    .forEach(influxDB -> queryResults.add(influxDB.query(query)));
            return queryResults;
        }
    }

    @Override
    public void query(Query query, Consumer<QueryResult> onSuccess, Consumer<Throwable> onFailure) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,onSuccess,onFailure);
            }
        }else{
            influxDBMap.values()
                    .forEach(influxDB -> influxDB.query(query,onSuccess,onFailure));
        }
    }

    @Override
    public void query(Query query, int chunkSize, Consumer<QueryResult> onNext) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,chunkSize,onNext);
            }
        }else{
            influxDBMap.values()
                    .forEach(influxDB ->influxDB.query(query,chunkSize,onNext));
        }

    }

    @Override
    public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,chunkSize,onNext);
            }
        }else{
            influxDBMap.values()
                    .forEach(influxDB -> influxDB.query(query,chunkSize,onNext));
        }

    }

    @Override
    public void query(Query query, int chunkSize, Consumer<QueryResult> onNext, Runnable onComplete) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,chunkSize,onNext,onComplete);
            }
        }else{
            influxDBMap.values()
                    .forEach(influxDB -> influxDB.query(query,chunkSize,onNext,onComplete));
        }

    }

    @Override
    public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext, Runnable onComplete) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,chunkSize,onNext,onComplete);
            }
        } else{
            influxDBMap.values()
                    .forEach(influxDB -> influxDB.query(query,chunkSize,onNext,onComplete));
        }

    }

    @Override
    public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext, Runnable onComplete, Consumer<Throwable> onFailure) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                influxDB.query(query,chunkSize,onNext,onComplete,onFailure);
            }
        }else{
            influxDBMap.values()
                    .forEach(influxDB -> influxDB.query(query,chunkSize,onNext,onComplete,onFailure));
        }

    }

    @Override
    public QueryResult query(Query query, TimeUnit timeUnit) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                return influxDB.query(query,timeUnit);
            }
            return null;
        }else{
            InfluxDB influxDB = new ArrayList<>(influxDBMap.values()).get(0);
            return influxDB.query(query,timeUnit);
        }
    }

    @Override
    public List<QueryResult> queries(Query query, TimeUnit timeUnit) {
        if (isDQL(query.getCommand())) {
            String measurement = extractMeasurementFromQuery(query.getCommand());
            if (measurement!=null) {
                InfluxDB influxDB = getCorrespondingConnection(measurement);
                return Collections.singletonList(influxDB.query(query,timeUnit));
            }
            return null;
        }else{
            List<QueryResult> queryResults = new ArrayList<>();
            influxDBMap.values()
                    .forEach(influxDB -> queryResults.add(influxDB.query(query,timeUnit)));
            return queryResults;
        }
    }

    @Override
    public void createDatabase(String name) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.createDatabase(name));
    }

    @Override
    public void deleteDatabase(String name) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.deleteDatabase(name));
    }

    @Override
    public List<String> describeDatabases() {
        List<String> descriptions = new ArrayList<>();
        influxDBMap.values()
                .forEach(influxDB -> {
                    String description = String.join(",", influxDB.describeDatabases());
                    descriptions.add(description);
                });
        return descriptions;
    }

    @Override
    public boolean databaseExists(String name) {
        List<Boolean> databaseExist = influxDBMap.values()
                .stream()
                .map(influxDB -> influxDB.databaseExists(name))
                .collect(Collectors.toList());
        return !databaseExist.contains(false);
    }

    public List<Boolean> databaseExistAll(String name) {
        return influxDBMap.values()
                .stream()
                .map(influxDB -> influxDB.databaseExists(name))
                .collect(Collectors.toList());
    }

    @Override
    public void flush() {
        influxDBMap.values()
                .forEach(InfluxDB::flush);
    }

    @Override
    public void close() {
        influxDBMap.values()
                .forEach(InfluxDB::close);
    }

    @Override
    public InfluxDB setConsistency(ConsistencyLevel consistency) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.setConsistency(consistency));
        return this;
    }

    @Override
    public InfluxDB setDatabase(String database) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.setDatabase(database));
        return this;
    }

    @Override
    public InfluxDB setRetentionPolicy(String retentionPolicy) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.setRetentionPolicy(retentionPolicy));
        return this;
    }

    @Override
    public void createRetentionPolicy(String rpName, String database, String duration, String shardDuration, int replicationFactor, boolean isDefault) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.createRetentionPolicy(rpName,database,duration,shardDuration,replicationFactor,isDefault));
    }

    @Override
    public void createRetentionPolicy(String rpName, String database, String duration, int replicationFactor, boolean isDefault) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.createRetentionPolicy(rpName,database,duration,replicationFactor,isDefault));
    }

    @Override
    public void createRetentionPolicy(String rpName, String database, String duration, String shardDuration, int replicationFactor) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.createRetentionPolicy(rpName,database,duration,shardDuration,replicationFactor));
    }

    @Override
    public void dropRetentionPolicy(String rpName, String database) {
        influxDBMap.values()
                .forEach(influxDB -> influxDB.dropRetentionPolicy(rpName,database));
    }
    private boolean isDQL(String query){
        return query.toUpperCase().contains("FROM");
    }
}
