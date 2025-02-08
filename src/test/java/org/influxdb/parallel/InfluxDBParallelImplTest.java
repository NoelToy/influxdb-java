package org.influxdb.parallel;

import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBParallelImpl;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class InfluxDBParallelImplTest {
    @Test
    public void getResultParallel(){
        List<String> serverURL = Arrays.asList("http://localhost:8086","http://localhost:8087");
        List<String> username =  Arrays.asList(System.getenv("INFLUXDB_1_USER"),System.getenv("INFLUXDB_2_USER"));
        List<String> password = Arrays.asList(System.getenv("INFLUXDB_1_PASSWORD"),System.getenv("INFLUXDB_2_PASSWORD"));
        List<String> databaseName = Arrays.asList("defaultdb","defaultdb");
        try (InfluxDBParallelImpl influxDB = (InfluxDBParallelImpl) InfluxDBFactory.connect(serverURL, username, password,databaseName)){
            //influxDB.setDatabase("defaultdb");
            Query query = new Query("SELECT * FROM \"512\"");
            QueryResult queryResult = influxDB.query(query);
            System.out.println(queryResult);
            /*Query queryTwo = new Query("SHOW MEASUREMENTS");
            List<QueryResult> queryResults= influxDB.queries(queryTwo);
            System.out.println(queryResults);*/
        }
        catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }

    @Test
    public void getResultParallelAsync(){
        List<String> serverURL = Arrays.asList("http://localhost:8086","http://localhost:8087");
        List<String> username =  Arrays.asList(System.getenv("INFLUXDB_1_USER"),System.getenv("INFLUXDB_2_USER"));
        List<String> password = Arrays.asList(System.getenv("INFLUXDB_1_PASSWORD"),System.getenv("INFLUXDB_2_PASSWORD"));
        List<String> databaseName = Arrays.asList("defaultdb","defaultdb");
        try (InfluxDBParallelImpl influxDB = (InfluxDBParallelImpl) InfluxDBFactory.connect(serverURL, username, password,databaseName)){
            Query query = new Query("SHOW MEASUREMENTS");
            Consumer<QueryResult> onSuccess = queryResult -> {
                System.out.println("Query succeeded!");
                queryResult.getResults().forEach(result -> {
                    if (result.getSeries() != null) {
                        result.getSeries().forEach(series -> {
                            System.out.println("Measurement: " + series.getName());
                            System.out.println("Columns: " + series.getColumns());
                            System.out.println("Values: " + series.getValues());
                        });
                    } else {
                        System.out.println("No data found.");
                    }
                });
            };
            Consumer<Throwable> onFailure = throwable -> {
                System.err.println("Query failed!");
                throwable.printStackTrace();
            };
            influxDB.query(query, onSuccess, onFailure);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }
    }
}
