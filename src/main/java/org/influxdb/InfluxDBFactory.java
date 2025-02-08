package org.influxdb;

import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.impl.InfluxDBImpl;

import okhttp3.OkHttpClient;
import org.influxdb.impl.InfluxDBParallelImpl;
import org.influxdb.impl.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * A Factory to create a instance of a InfluxDB Database adapter.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public enum InfluxDBFactory {
  INSTANCE;

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url
   *            the url to connect to.
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url) {
    Preconditions.checkNonEmptyString(url, "url");
    return new InfluxDBImpl(url, null, null, new OkHttpClient.Builder());
  }

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url
   *            the url to connect to.
   * @param username
   *            the username which is used to authorize against the influxDB instance.
   * @param password
   *            the password for the username which is used to authorize against the influxDB
   *            instance.
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url, final String username, final String password) {
    Preconditions.checkNonEmptyString(url, "url");
    Preconditions.checkNonEmptyString(username, "username");
    return new InfluxDBImpl(url, username, password, new OkHttpClient.Builder());
  }

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url
   *            the url to connect to.
   * @param client
   *            the HTTP client to use
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url, final OkHttpClient.Builder client) {
    Preconditions.checkNonEmptyString(url, "url");
    Objects.requireNonNull(client, "client");
    return new InfluxDBImpl(url, null, null, client);
  }

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url
   *            the url to connect to.
   * @param username
   *            the username which is used to authorize against the influxDB instance.
   * @param password
   *            the password for the username which is used to authorize against the influxDB
   *            instance.
   * @param client
   *            the HTTP client to use
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url, final String username, final String password,
      final OkHttpClient.Builder client) {
    return connect(url, username, password, client, ResponseFormat.JSON);
  }

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url
   *            the url to connect to.
   * @param username
   *            the username which is used to authorize against the influxDB instance.
   * @param password
   *            the password for the username which is used to authorize against the influxDB
   *            instance.
   * @param client
   *            the HTTP client to use
   * @param responseFormat
   *            The {@code ResponseFormat} to use for response from InfluxDB server
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url, final String username, final String password,
      final OkHttpClient.Builder client, final ResponseFormat responseFormat) {
    Preconditions.checkNonEmptyString(url, "url");
    Preconditions.checkNonEmptyString(username, "username");
    Objects.requireNonNull(client, "client");
    return new InfluxDBImpl(url, username, password, client, responseFormat);
  }
  /**
   * Create parallel connections to a InfluxDB.
   *
   * @param urls
   *            the list of urls to connect to.
   * @param usernames
   *            the list of usernames which is used to authorize against each influxDB instance.
   * @param passwords
   *            the list of passwords for the username which is used to authorize against each influxDB
   *            instance.
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final List<String> urls, final List<String> usernames, final List<String> passwords,final List<String> databases) {
    List<InfluxDB> influxDBConnections = new ArrayList<>();
    for(String url : urls){
      Preconditions.checkNonEmptyString(url, "url");
    }
    for(String username : usernames){
      Preconditions.checkNonEmptyString(username, "username");
    }
    for (int i = 0; i < urls.size(); i++) {
      influxDBConnections.add(new InfluxDBImpl(urls.get(i), usernames.get(i), passwords.get(i),new OkHttpClient.Builder()));
    }
    return new InfluxDBParallelImpl(influxDBConnections,databases);
  }
}
