package org.influxdb.parallel;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBNormalImplTest {
    @Test
    public void testInfluxDBConnection(){
        System.out.println("Influx Test");
        final String serverURL = "http://localhost:8087", username = System.getenv("INFLUXDB_2_USER"), password = System.getenv("INFLUXDB_2_PASSWORD"),databaseName="defaultdb";
        try (InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {
            influxDB.setDatabase(databaseName);
            QueryResult queryResult = influxDB.query(new Query("SHOW MEASUREMENTS"));
            System.out.println(queryResult);
            Runtime.getRuntime().addShutdownHook(new Thread(influxDB::close));

        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }
    @Test
    public void writeDataToMeasurement(){
        System.out.println("Influx Test");
        final String serverURL = "http://localhost:8087", username = System.getenv("INFLUXDB_2_USER"), password = System.getenv("INFLUXDB_2_PASSWORD"),databaseName="defaultdb";
        try (InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {
            influxDB.setDatabase(databaseName);
            Point pointOne = Point.measurement("288").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("location", "sherman")
                    .addField("level description", "below 10 feet")
                    .addField("water_level", 7.962d)
                    .build();
            Point pointTwo = Point.measurement("288")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("location", "lyford")
                    .addField("level description", "above 17 feet")
                    .addField("water_level", 18.637d)
                    .build();
            influxDB.write(databaseName, "one_day_only", pointOne);
            influxDB.write(databaseName, "one_day_only", pointTwo);

        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }

    @Test
    public void readDataFromMeasurements(){
        System.out.println("Influx Test");
        final String serverURL = "http://localhost:8086", username = System.getenv("INFLUXDB_1_USER"), password = System.getenv("INFLUXDB_1_PASSWORD"),databaseName="defaultdb";
        try (InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {
            influxDB.setDatabase(databaseName);
            Query query = new Query("SELECT * FROM \"475\"");
            System.out.println(extractMeasurementFromQuery(query.getCommand()));
            System.out.println(query.getCommand());
            QueryResult queryResult = influxDB.query(query);
            System.out.println(queryResult);
        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }

    @Test
    public void getAllMeasurements(){
        final String serverURL = "http://localhost:8087", username = System.getenv("INFLUXDB_2_USER"), password = System.getenv("INFLUXDB_2_PASSWORD"),databaseName="defaultdb";
        try (InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {
            influxDB.setDatabase(databaseName);
            QueryResult queryResult = influxDB.query(new Query("SHOW MEASUREMENTS"));
            List<String> measurements = extractMeasurements(queryResult);
            System.out.println(measurements);
        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }


    @Test
    public void executeQueries(){
        final String serverURL = "http://localhost:8087", username = System.getenv("INFLUXDB_1_USER"), password = System.getenv("INFLUXDB_1_PASSWORD"),databaseName="defaultdb";
        String query = "CREATE RETENTION POLICY one_day_only  ON "+databaseName+" DURATION 1d REPLICATION 1 DEFAULT";
        try (InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password)) {
            influxDB.setDatabase(databaseName);
            QueryResult queryResult = influxDB.query(new Query(query));
            System.out.println(queryResult);
        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
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

    private String extractMeasurementFromQuery(String sql) {
        sql = sql.trim().toUpperCase();
        int fromIndex = sql.indexOf("FROM");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid InfluxDB DQL query: 'FROM' keyword not found.");
        }
        String afterFrom = sql.substring(fromIndex + 4).trim();
        String[] tokens = afterFrom.split("\\s+");
        return tokens.length > 0 ? tokens[0] : null;
    }
}
