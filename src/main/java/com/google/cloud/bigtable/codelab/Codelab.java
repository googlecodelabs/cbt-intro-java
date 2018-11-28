/**
 * Copyright 2018 Google LLC. All Rights Reserved.
 *
 * <p>Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.codelab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;

import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Codelab {
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
  private static final byte[] LAT_COLUMN_NAME = Bytes.toBytes("VehicleLocation.Latitude");
  private static final byte[] LONG_COLUMN_NAME = Bytes.toBytes("VehicleLocation.Longitude");
  private static final String[] MANHATTAN_BUS_LINES =
      ("M1,M2,M3,M4,M5,M7,M8,M9,M10,M11,M12,M15,M20,M21,M22,M31,M35,M42,M50,M55,M57,M66,M72,M96,"
              + "M98,M100,M101,M102,M103,M104,M106,M116,M14A,M34A-SBS,M14D,M15-SBS,M23-SBS,"
              + "M34-SBS,M60-SBS,M79-SBS,M86-SBS")
          .split(",");

  /** Connects to Cloud Bigtable, runs a query and prints the results. */
  private static void runQuery(
      String projectId, String instanceId, String tableName, String query) {
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      // Retrieve the table we just created so we can do some reads and writes
      Table table = connection.getTable(TableName.valueOf(tableName));

      switch (query) {
        case "lookupVehicleInGivenHour":
          lookupVehicleInGivenHour(table);
          break;
        case "scanBusLineInGivenHour":
          scanBusLineInGivenHour(table);
          break;
        case "scanEntireBusLine":
          scanEntireBusLine(table);
          break;
        case "filterBusesGoingEast":
          filterBusesGoingEast(table);
          break;
        case "filterBusesGoingWest":
          filterBusesGoingWest(table);
          break;
        case "scanManhattanBusesInGivenHour":
          scanManhattanBusesInGivenHour(table);
          break;
        default:
          System.err.println(
              "Please provide one of the following queries: lookupVehicleInGivenHour, "
                  + "scanBusLineInGivenHour, scanEntireBusLine, filterBusesGoingEast, "
                  + "filterBusesGoingWest, scanManhattanBusesInGivenHour.");
          System.exit(1);
      }

    } catch (IOException e) {
      System.err.println("Exception while running Codelab: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }

  private static void lookupVehicleInGivenHour(Table table) throws IOException {
    String rowKey = "MTA/M86-SBS/1496275200000/NYCT_5824";
    Result getResult =
        table.get(
            new Get(Bytes.toBytes(rowKey))
                .setMaxVersions(Integer.MAX_VALUE)
                .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
                .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME));
    System.out.print(
        "Lookup a specific vehicle on the M86 route on June 1, 2017 from 12:00am to 1:00am:");
    printLatLongPairs(getResult);
  }

  private static void filterBusesGoingEast(Table table) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter valueFilter =
        new SingleColumnValueFilter(
            COLUMN_FAMILY_NAME,
            Bytes.toBytes("DestinationName"),
            CompareOp.EQUAL,
            Bytes.toBytes("Select Bus Service Yorkville East End AV"));
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"))
        .setFilter(valueFilter);

    System.out.print("Scan for all m86 heading East during the month:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static void filterBusesGoingWest(Table table) throws IOException {
    SingleColumnValueFilter valueFilter =
        new SingleColumnValueFilter(
            COLUMN_FAMILY_NAME,
            Bytes.toBytes("DestinationName"),
            CompareOp.EQUAL,
            Bytes.toBytes("Select Bus Service Westside West End AV"));

    Scan scan = new Scan();
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"))
        .setFilter(valueFilter);

    System.out.print("Scan for all m86 heading West during the month:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static void scanBusLineInGivenHour(Table table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/1496275200000"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/1496275200000"));
    System.out.print("Scan for all M86 buses on June 1, 2017 from 12:00am to 1:00am:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static void scanEntireBusLine(Table table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(1)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M86-SBS/"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M86-SBS/"));

    System.out.print("Scan for all m86 during the month:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static void scanManhattanBusesInGivenHour(Table table) throws IOException {
    List<RowRange> ranges = new ArrayList<>();

    for (String busLine : MANHATTAN_BUS_LINES) {
      ranges.add(
          new RowRange(
              Bytes.toBytes("MTA/" + busLine + "/1496275200000"), true,
              Bytes.toBytes("MTA/" + busLine + "/1496275200001"), false));
    }
    Filter filter = new MultiRowRangeFilter(ranges);

    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE)
        .addColumn(COLUMN_FAMILY_NAME, LAT_COLUMN_NAME)
        .addColumn(COLUMN_FAMILY_NAME, LONG_COLUMN_NAME)
        .withStartRow(Bytes.toBytes("MTA/M"))
        .setRowPrefixFilter(Bytes.toBytes("MTA/M"))
        .setFilter(filter);

    System.out.print("Scan for all buses on June 1, 2017 from 12:00am to 1:00am:");
    ResultScanner scanner = table.getScanner(scan);
    for (Result row : scanner) {
      printLatLongPairs(row);
    }
  }

  private static void printLatLongPairs(Result result) {
    Cell[] raw = result.rawCells();
    assert (raw.length % 2 == 0);
    for (int i = 0; i < raw.length / 2; i++) {
      System.out.print(Bytes.toString(raw[i].getValueArray()));
      System.out.print(",");
      System.out.println(Bytes.toString(raw[i + raw.length / 2].getValueArray()));
    }
  }

  public static void main(String[] args) {
    // Consult system properties to get project/instance
    String projectId = requiredProperty("bigtable.projectID");
    String instanceId = requiredProperty("bigtable.instanceID");
    String table = requiredProperty("bigtable.table");
    String query = requiredProperty("query");

    runQuery(projectId, instanceId, table, query);
  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    if (value == null) {
      throw new IllegalArgumentException("Missing required system property: " + prop);
    }
    return value;
  }
}
