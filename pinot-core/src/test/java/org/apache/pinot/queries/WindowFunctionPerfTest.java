/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test cases for verifying that window functions are producing correct results. Note that SQL script
 * pinot-core/src/test/resources/WindowFunctionQueriesTest.sql was used to create a mysql table for verifying these
 * results.
 *
 * Window Function query processing involves merging segment-level intermediate results into a server-level
 * DataTable. DataTables from different servers are then merged on Broker to create the final result. The order of
 * results produced by merge is not deterministic. To make the result order deterministic (so that results can be
 * easily verified), we add "ORDER by fname" to the end of all SQL statements being tested here.
*/

public class WindowFunctionPerfTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "WindowFunctionQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_LEFT = "leftSegment";
  private static final String SEGMENT_NAME_RIGHT = "rightSegment";
  private static final int NUM_RECORDS = 20;

  private static final String LNAME_COlUMN = "lname";
  private static final String FNAME_COLUMN = "fname";
  private static final String SCORE_COLUMN = "score";
  private static final String MONEY_COLUMN = "money";
  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final org.apache.pinot.spi.data.Schema SCHEMA =
      new org.apache.pinot.spi.data.Schema.SchemaBuilder().addSingleValueDimension(LNAME_COlUMN,
              FieldSpec.DataType.STRING).addSingleValueDimension(FNAME_COLUMN, FieldSpec.DataType.STRING)
          .addSingleValueDimension(SCORE_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(MONEY_COLUMN, FieldSpec.DataType.FLOAT)
          .addDateTime(TIMESTAMP_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIMESTAMP_COLUMN)
          .setTimeType("MILLISECONDS").setNumReplicas(1).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  GenericRow createRecord(int score, float money, String fname, String lname, long timestamp) {
    GenericRow record = new GenericRow();
    record.putValue(LNAME_COlUMN, lname);
    record.putValue(FNAME_COLUMN, fname);
    record.putValue(SCORE_COLUMN, score);
    record.putValue(MONEY_COLUMN, money);
    record.putValue(TIMESTAMP_COLUMN, timestamp);
    return record;
  }

  private void createSegment(List<GenericRow> records, String segmentName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records1 = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < 50000; i++) {
      records1.add(createRecord(120, 200.50F, "albert1", "albert", 1643666769000L + i));
      records1.add(createRecord(250, 32.50F, "martian1", "mouse", 1643666728000L + i));
      records1.add(createRecord(310, -44.50F, "martian2", "mouse", 1643666432000L + i));
      records1.add(createRecord(340, 11.50F, "donald1", "duck", 1643666726000L + i));
      records1.add(createRecord(110, 16, "goofy1", "goofy", 1643667762000L + i));
      records1.add(createRecord(150, 12, "goofy2", "goofy", 1643667762000L + i));
      records1.add(createRecord(100, -28, "daffy1", "daffy", 1643667092000L + i));
      records1.add(createRecord(120, -16, "pluto1", "dwag", 1643666712000L + i));
      records1.add(createRecord(120, -16, "zebra1", "zookeeper", 1643666712000L + i));
      records1.add(createRecord(220, -16, "zebra2", "zookeeper", 1643666712000L + i));
    }
    createSegment(records1, SEGMENT_NAME_LEFT);
    ImmutableSegment immutableSegment1 =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LEFT), ReadMode.mmap);

    List<GenericRow> records2 = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < 50000; i++) {
      records2.add(createRecord(150, 10.50F, "alice1", "wonderland", 1650069985000L + i));
      records2.add(createRecord(200, 1.50F, "albert2", "albert", 1650050085000L + i));
      records2.add(createRecord(32, 10.0F, "mickey1", "mouse", 1650040085000L + i));
      records2.add(createRecord(-40, 250F, "minney2", "mouse", 1650043085000L + i));
      records2.add(createRecord(10, 4.50F, "donald2", "duck", 1650011085000L + i));
      records2.add(createRecord(5, 7.50F, "goofy3", "duck", 1650010085000L + i));
      records2.add(createRecord(5, 4.50F, "daffy2", "duck", 1650045085000L + i));
      records2.add(createRecord(10, 46.0F, "daffy3", "duck", 1650032085000L + i));
      records2.add(createRecord(20, 20.5F, "goofy4", "goofy", 1650011085000L + i));
      records2.add(createRecord(-20, 2.5F, "pluto2", "dwag", 1650052285000L + i));
    }
    createSegment(records2, SEGMENT_NAME_RIGHT);
    ImmutableSegment immutableSegment2 =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_RIGHT), ReadMode.mmap);

    _indexSegment = null;
    _indexSegments = Arrays.asList(immutableSegment1, immutableSegment2);
  }

  @Test
  public void perfWindowFunctionQuery() {
    String query = "";
    query += "SELECT fname, lname, max(score) over(order by lname) FROM testTable LIMIT 1000000";
    System.out.println("Time, Rows");
    long avg = 0, count = 0;
    for (int i = 0; i < 21; i++) {
      long start = System.currentTimeMillis();
      BrokerResponseNative response = getBrokerResponse(query, true);
      long elapsed = (System.currentTimeMillis() - start);
      System.out.println(elapsed);
      if (i > 0) {
        avg += elapsed;
        ++count;
      }
//
//      List<Object[]> rows = response.getResultTable().getRows();
//      for (int k = 0; k < rows.size(); k++) {
//        Object[] actual = rows.get(i);
//        print(actual);
//      }
    }
    System.out.println("Average: " + (avg/count));
  }

  @Test
  public void perfOrderByQuery() {
    String query = "";
    query += "SELECT fname, lname, score FROM testTable ORDER BY lname LIMIT 1000000";
    System.out.println("Time, Rows");
    long avg = 0, count = 0;
    for (int i = 0; i < 21; i++) {
      long start = System.currentTimeMillis();
      BrokerResponseNative response = getBrokerResponse(query, true);
      long elapsed = (System.currentTimeMillis() - start);
      System.out.println(elapsed);
      if (i > 0) {
        avg += elapsed;
        ++count;
      }
    }
    System.out.println("Average: " + (avg/count));
  }


  private void print(Object[] array) {
    System.out.print("{");
    for (int i = 0; i < array.length; i++) {
      Object o = array[i];
      if (i > 0) {
        System.out.print(", ");
      }
      if (o instanceof String) {
        System.out.print("\"" + o + "\"");
      } else {
        System.out.print(o);
      }
    }
    System.out.print("}, ");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
