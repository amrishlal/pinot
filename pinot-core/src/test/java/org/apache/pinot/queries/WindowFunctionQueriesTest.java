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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.testng.Assert;
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

public class WindowFunctionQueriesTest extends BaseQueriesTest {

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
    records1.add(createRecord(120, 200.50F, "albert1", "albert", 1643666769000L));
    records1.add(createRecord(250, 32.50F, "martian1", "mouse", 1643666728000L));
    records1.add(createRecord(310, -44.50F, "martian2", "mouse", 1643666432000L));
    records1.add(createRecord(340, 11.50F, "donald1", "duck", 1643666726000L));
    records1.add(createRecord(110, 16, "goofy1", "goofy", 1643667762000L));
    records1.add(createRecord(150, 12, "goofy2", "goofy", 1643667762000L));
    records1.add(createRecord(100, -28, "daffy1", "daffy", 1643667092000L));
    records1.add(createRecord(120, -16, "pluto1", "dwag", 1643666712000L));
    records1.add(createRecord(120, -16, "zebra1", "zookeeper", 1643666712000L));
    records1.add(createRecord(220, -16, "zebra2", "zookeeper", 1643666712000L));
    createSegment(records1, SEGMENT_NAME_LEFT);
    ImmutableSegment immutableSegment1 =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LEFT), ReadMode.mmap);

    List<GenericRow> records2 = new ArrayList<>(NUM_RECORDS);
    records2.add(createRecord(150, 10.50F, "alice1", "wonderland", 1650069985000L));
    records2.add(createRecord(200, 1.50F, "albert2", "albert", 1650050085000L));
    records2.add(createRecord(32, 10.0F, "mickey1", "mouse", 1650040085000L));
    records2.add(createRecord(-40, 250F, "minney2", "mouse", 1650043085000L));
    records2.add(createRecord(10, 4.50F, "donald2", "duck", 1650011085000L));
    records2.add(createRecord(5, 7.50F, "goofy3", "duck", 1650010085000L));
    records2.add(createRecord(5, 4.50F, "daffy2", "duck", 1650045085000L));
    records2.add(createRecord(10, 46.0F, "daffy3", "duck", 1650032085000L));
    records2.add(createRecord(20, 20.5F, "goofy4", "goofy", 1650011085000L));
    records2.add(createRecord(-20, 2.5F, "pluto2", "dwag", 1650052285000L));
    createSegment(records2, SEGMENT_NAME_RIGHT);
    ImmutableSegment immutableSegment2 =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_RIGHT), ReadMode.mmap);

    _indexSegment = null;
    _indexSegments = Arrays.asList(immutableSegment1, immutableSegment2);
  }

  /** Value of window function, without PARTITION BY and ORDER BY, should not change with low LIMIT clause value. **/
  @Test
  public void testSimple() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 110.6}, {"albert2", "albert", 110.6}, {"alice1", "wonderland", 110.6}, {"daffy1",
            "daffy", 110.6}, {"daffy2", "duck", 110.6}, {"daffy3", "duck", 110.6}, {"donald1", "duck", 110.6}, {
            "donald2", "duck", 110.6}, {"goofy1", "goofy", 110.6}, {"goofy2", "goofy", 110.6}, {"goofy3", "duck",
            110.6}, {"goofy4", "goofy", 110.6}, {"martian1", "mouse", 110.6}, {"martian2", "mouse", 110.6}, {"mickey1",
            "mouse", 110.6}, {"minney2", "mouse", 110.6}, {"pluto1", "dwag", 110.6}, {"pluto2", "dwag", 110.6}, {
            "zebra1", "zookeeper", 110.6}, {"zebra2", "zookeeper", 110.6}};
    checkResults("select fname, lname, avg(score) over() from testTable ORDER BY fname ASC limit 100", expecteds1,
        false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 110.6}, {"albert2", "albert", 110.6}, {"alice1", "wonderland", 110.6}, {"daffy1",
            "daffy", 110.6}, {"daffy2", "duck", 110.6}, {"daffy3", "duck", 110.6}, {"donald1", "duck", 110.6}, {
            "donald2", "duck", 110.6}, {"goofy1", "goofy", 110.6}, {"goofy2", "goofy", 110.6}};
    checkResults("select fname, lname, avg(score) over() from testTable ORDER BY fname ASC limit 10", expecteds2,
        false);

    Object[][] expecteds3 =
        {{"albert1", "albert", 110.6}, {"albert2", "albert", 110.6}};
    checkResults("select fname, lname, avg(score) over() from testTable ORDER BY fname ASC limit 2", expecteds3,
        false);

    Object[][] expecteds4 =
        {{"albert1", "albert", 25.475}, {"albert2", "albert", 25.475}, {"alice1", "wonderland", 25.475}, {"daffy1",
            "daffy", 25.475}, {"daffy2", "duck", 25.475}, {"daffy3", "duck", 25.475}, {"donald1", "duck", 25.475}, {
            "donald2", "duck", 25.475}, {"goofy1", "goofy", 25.475}, {"goofy2", "goofy", 25.475}, {"goofy3", "duck",
            25.475}, {"goofy4", "goofy", 25.475}, {"martian1", "mouse", 25.475}, {"martian2", "mouse", 25.475}, {
            "mickey1", "mouse", 25.475}, {"minney2", "mouse", 25.475}, {"pluto1", "dwag", 25.475}, {"pluto2", "dwag",
            25.475}, {"zebra1", "zookeeper", 25.475}, {"zebra2", "zookeeper", 25.475}};
    checkResults("select fname, lname, avg(money) over() from testTable ORDER BY fname ASC limit 100", expecteds4,
        false);
  }

  @Test
  public void testSimpleWindowFunctionsWithAlias() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 110.6}, {"albert2", "albert", 110.6}, {"alice1", "wonderland", 110.6}, {"daffy1",
            "daffy", 110.6}, {"daffy2", "duck", 110.6}, {"daffy3", "duck", 110.6}, {"donald1", "duck", 110.6}, {
            "donald2", "duck", 110.6}, {"goofy1", "goofy", 110.6}, {"goofy2", "goofy", 110.6}, {"goofy3", "duck",
            110.6}, {"goofy4", "goofy", 110.6}, {"martian1", "mouse", 110.6}, {"martian2", "mouse", 110.6}, {"mickey1",
            "mouse", 110.6}, {"minney2", "mouse", 110.6}, {"pluto1", "dwag", 110.6}, {"pluto2", "dwag", 110.6}, {
            "zebra1", "zookeeper", 110.6}, {"zebra2", "zookeeper", 110.6}};
    checkResults("select fname, lname, avg(score) over() as x from testTable ORDER BY fname ASC limit 100", expecteds1,
        false);
  }

  @Test
  public void testMultipleWindowFunctions() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 110.6, 25.475}, {"albert2", "albert", 110.6, 25.475}, {"alice1", "wonderland", 110.6,
            25.475}, {"daffy1", "daffy", 110.6, 25.475}, {"daffy2", "duck", 110.6, 25.475}, {"daffy3", "duck", 110.6,
            25.475}, {"donald1", "duck", 110.6, 25.475}, {"donald2", "duck", 110.6, 25.475}, {"goofy1", "goofy",
            110.6, 25.475}, {"goofy2", "goofy", 110.6, 25.475}, {"goofy3", "duck", 110.6, 25.475}, {"goofy4", "goofy",
            110.6, 25.475}, {"martian1", "mouse", 110.6, 25.475}, {"martian2", "mouse", 110.6, 25.475}, {"mickey1",
            "mouse", 110.6, 25.475}, {"minney2", "mouse", 110.6, 25.475}, {"pluto1", "dwag", 110.6, 25.475}, {"pluto2",
            "dwag", 110.6, 25.475}, {"zebra1", "zookeeper", 110.6, 25.475}, {"zebra2", "zookeeper", 110.6, 25.475}};
    checkResults(
        "select fname, lname, avg(score) over(), avg(money) over() from testTable ORDER BY fname ASC limit 100",
        expecteds1, false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 110.6, 160.0}, {"albert2", "albert", 110.6, 160.0}, {"alice1", "wonderland", 110.6,
            104.0}, {"daffy1", "daffy", 110.6, 140.0}, {"daffy2", "duck", 110.6, 98.75}, {"daffy3", "duck", 110.6,
            98.75}, {"donald1", "duck", 110.6, 98.75}, {"donald2", "duck", 110.6, 98.75}, {"goofy1", "goofy", 110.6,
            90.0}, {"goofy2", "goofy", 110.6, 90.0}, {"goofy3", "duck", 110.6, 98.75}, {"goofy4", "goofy", 110.6,
            90.0}, {"martian1", "mouse", 110.6, 101.29411764705883}, {"martian2", "mouse", 110.6, 101.29411764705883},
            {"mickey1", "mouse", 110.6, 101.29411764705883}, {"minney2", "mouse", 110.6, 101.29411764705883}, {
            "pluto1", "dwag", 110.6, 89.0}, {"pluto2", "dwag", 110.6, 89.0}, {"zebra1", "zookeeper", 110.6, 110.6}, {
            "zebra2", "zookeeper", 110.6, 110.6}};
    checkResults(
        "select fname, lname, avg(score) over(), avg(score) over(order by lname) from testTable ORDER BY fname ASC "
            + "limit 100", expecteds2, false);

    Object[][] expecteds3 =
        {{"albert1", "albert", 110.6, 160.0}, {"albert2", "albert", 110.6, 160.0}, {"alice1", "wonderland", 110.6,
            104.0}, {"daffy1", "daffy", 110.6, 140.0}, {"daffy2", "duck", 110.6, 98.75}};
    checkResults(
        "select fname, lname, avg(score) over(), avg(score) over(order by lname) from testTable ORDER BY fname ASC "
            + "limit 5", expecteds3, false);
  }

  @Test
  public void testMultipleMixedWindowFunctions() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0, 25.475}, {"albert2", "albert", 160.0, 25.475}, {"alice1", "wonderland", 104.0,
            25.475}, {"daffy1", "daffy", 140.0, 25.475}, {"daffy2", "duck", 98.75, 25.475}, {"daffy3", "duck", 98.75,
            25.475}, {"donald1", "duck", 98.75, 25.475}, {"donald2", "duck", 98.75, 25.475}, {"goofy1", "goofy", 90.0,
            25.475}, {"goofy2", "goofy", 90.0, 25.475}, {"goofy3", "duck", 98.75, 25.475}, {"goofy4", "goofy", 90.0,
            25.475}, {"martian1", "mouse", 101.29411764705883, 25.475}, {"martian2", "mouse", 101.29411764705883,
            25.475}, {"mickey1", "mouse", 101.29411764705883, 25.475}, {"minney2", "mouse", 101.29411764705883,
            25.475}, {"pluto1", "dwag", 89.0, 25.475}, {"pluto2", "dwag", 89.0, 25.475}, {"zebra1", "zookeeper",
            110.6, 25.475}, {"zebra2", "zookeeper", 110.6, 25.475}};
    checkResults("select fname, lname, avg(score) over(order by lname), avg(money) over() from testTable ORDER "
        + "BY fname ASC limit 100", expecteds1, false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 160.0, 25.475}, {"albert2", "albert", 160.0, 25.475}, {"alice1", "wonderland", 104.0,
            25.475}, {"daffy1", "daffy", 140.0, 25.475}, {"daffy2", "duck", 98.75, 25.475}};
    checkResults("select fname, lname, avg(score) over(order by lname), avg(money) over() from testTable ORDER BY "
            + "fname ASC limit 5",
        expecteds2, false);
  }

  @Test
  public void testOrderByOneColumn() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0}, {"albert2", "albert", 160.0}, {"alice1", "wonderland", 104.0}, {"daffy1",
            "daffy", 140.0}, {"daffy2", "duck", 98.75}, {"daffy3", "duck", 98.75}, {"donald1", "duck", 98.75}, {
            "donald2", "duck", 98.75}, {"goofy1", "goofy", 90.0}, {"goofy2", "goofy", 90.0}, {"goofy3", "duck", 98.75},
            {"goofy4", "goofy", 90.0}, {"martian1", "mouse", 101.29411764705883}, {"martian2", "mouse",
            101.29411764705883}, {"mickey1", "mouse", 101.29411764705883}, {"minney2", "mouse", 101.29411764705883},
            {"pluto1", "dwag", 89.0}, {"pluto2", "dwag", 89.0}, {"zebra1", "zookeeper", 110.6}, {"zebra2", "zookeeper",
            110.6}};
    checkResults("select fname, lname, avg(score) over(order by lname) from testTable ORDER BY fname ASC limit 100",
        expecteds1, false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 101.0}, {"albert2", "albert", 101.0}, {"alice1", "wonderland", 30.083333333333332}, {
            "daffy1", "daffy", 58.0}, {"daffy2", "duck", 31.0}, {"daffy3", "duck", 31.0}, {"donald1", "duck", 31.0}, {
            "donald2", "duck", 31.0}, {"goofy1", "goofy", 21.76923076923077}, {"goofy2", "goofy", 21.76923076923077}, {
            "goofy3", "duck", 31.0}, {"goofy4", "goofy", 21.76923076923077}, {"martian1", "mouse", 31.235294117647058},
            {"martian2", "mouse", 31.235294117647058}, {"mickey1", "mouse", 31.235294117647058}, {"minney2", "mouse",
            31.235294117647058}, {"pluto1", "dwag", 23.45}, {"pluto2", "dwag", 23.45}, {"zebra1", "zookeeper",
            25.475}, {"zebra2", "zookeeper", 25.475}};
    checkResults("select fname, lname, avg(money) over(order by lname) from testTable ORDER BY fname ASC limit 100",
        expecteds2, false);
  }

  @Test
  public void testOrderByTwoColumn() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 120.0}, {"albert2", "albert", 160.0}, {"alice1", "wonderland", 104.0}, {"daffy1",
            "daffy", 140.0}, {"daffy2", "duck", 106.25}, {"daffy3", "duck", 87.0}, {"donald1", "duck",
            129.16666666666666}, {"donald2", "duck", 112.14285714285714}, {"goofy1", "goofy", 90.9090909090909}, {
            "goofy2", "goofy", 95.83333333333333}, {"goofy3", "duck", 98.75}, {"goofy4", "goofy", 90.0}, {"martian1",
            "mouse", 101.42857142857143}, {"martian2", "mouse", 115.33333333333333}, {"mickey1", "mouse", 110.125}, {
            "minney2", "mouse", 101.29411764705883}, {"pluto1", "dwag", 101.11111111111111}, {"pluto2", "dwag", 89.0},
            {"zebra1", "zookeeper", 104.84210526315789}, {"zebra2", "zookeeper", 110.6}};
    checkResults("select fname, lname, avg(score) over(order by lname, fname) from testTable ORDER BY fname ASC limit "
        + "100", expecteds1, false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 200.5}, {"albert2", "albert", 101.0}, {"alice1", "wonderland", 30.083333333333332},
            {"daffy1", "daffy", 58.0}, {"daffy2", "duck", 44.625}, {"daffy3", "duck", 44.9},
            {"donald1", "duck", 39.333333333333336}, {"donald2", "duck", 34.357142857142854},
            {"goofy1", "goofy", 22.772727272727273}, {"goofy2", "goofy", 21.875}, {"goofy3", "duck", 31.0},
            {"goofy4", "goofy", 21.76923076923077}, {"martian1", "mouse", 22.535714285714285},
            {"martian2", "mouse", 18.066666666666666}, {"mickey1", "mouse", 17.5625},
            {"minney2", "mouse", 31.235294117647058}, {"pluto1", "dwag", 25.77777777777778},
            {"pluto2", "dwag", 23.45}, {"zebra1", "zookeeper", 27.657894736842106}, {"zebra2", "zookeeper", 25.475}};
    checkResults("select lname, fname, avg(money) over(order by lname, fname) from testTable ORDER BY fname ASC limit "
        + "100", expecteds2, false);
  }

  @Test
  public void testPartition() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0}, {"albert2", "albert", 160.0}, {"alice1", "wonderland", 150.0}, {"daffy1",
            "daffy", 100.0}, {"daffy2", "duck", 74.0}, {"daffy3", "duck", 74.0}, {"donald1", "duck", 74.0}, {"donald2",
            "duck", 74.0}, {"goofy1", "goofy", 93.33333333333333}, {"goofy2", "goofy", 93.33333333333333}, {"goofy3",
            "duck", 74.0}, {"goofy4", "goofy", 93.33333333333333}, {"martian1", "mouse", 138.0}, {"martian2",
            "mouse", 138.0}, {"mickey1", "mouse", 138.0}, {"minney2", "mouse", 138.0}, {"pluto1", "dwag", 50.0}, {
            "pluto2", "dwag", 50.0}, {"zebra1", "zookeeper", 170.0}, {"zebra2", "zookeeper", 170.0}};
    checkResults("select fname, lname, avg(score) over(partition by lname) from testTable ORDER BY fname ASC limit 100",
        expecteds1, false);

    Object[][] expecteds2 =
        {{"albert1", "albert", 160.0}, {"albert2", "albert", 160.0}, {"alice1", "wonderland", 150.0}, {"daffy1",
            "daffy", 100.0}, {"daffy2", "duck", 74.0}};
    checkResults("select fname, lname, avg(score) over(partition by lname) from testTable ORDER BY fname ASC limit 5",
        expecteds2, false);
  }

  @Test
  public void testPartitionWithOrder() {
    Object[][] expecteds1 =
        {{"albert", "albert1", 120.0}, {"albert", "albert2", 160.0}, {"daffy", "daffy1", 100.0}, {"duck", "daffy2",
            5.0}, {"duck", "daffy3", 7.5}, {"duck", "donald1", 118.33333333333333}, {"duck", "donald2", 91.25}, {
            "duck", "goofy3", 74.0}, {"dwag", "pluto1", 120.0}, {"dwag", "pluto2", 50.0}, {"goofy", "goofy1", 110.0}, {
            "goofy", "goofy2", 130.0}, {"goofy", "goofy4", 93.33333333333333}, {"mouse", "martian1", 250.0}, {"mouse",
            "martian2", 280.0}, {"mouse", "mickey1", 197.33333333333334}, {"mouse", "minney2", 138.0}, {"wonderland",
            "alice1", 150.0}, {"zookeeper", "zebra1", 120.0}, {"zookeeper", "zebra2", 170.0}};

    checkResults("select fname, lname, avg(score) over(partition by lname order by fname) from testTable limit 100",
        expecteds1, false);

    Object[][] expecteds2 =
        {{"albert", "albert1", 120.0}, {"albert", "albert2", 160.0}, {"daffy", "daffy1", 100.0}, {"duck", "daffy2",
            5.0}, {"duck", "daffy3", 7.5}};

    checkResults("select fname, lname, avg(score) over(partition by lname order by fname) from testTable limit 5",
        expecteds2, false);
  }

  @Test
  public void testBrokerMergeSimple() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 110.6}, {"albert1", "albert", 110.6}, {"albert2", "albert", 110.6}, {"albert2",
            "albert", 110.6}, {"alice1", "wonderland", 110.6}, {"alice1", "wonderland", 110.6}, {"daffy1", "daffy",
            110.6}, {"daffy1", "daffy", 110.6}, {"daffy2", "duck", 110.6}, {"daffy2", "duck", 110.6}, {"daffy3",
            "duck", 110.6}, {"daffy3", "duck", 110.6}, {"donald1", "duck", 110.6}, {"donald1", "duck", 110.6}, {
          "donald2", "duck", 110.6}, {"donald2", "duck", 110.6}, {"goofy1", "goofy", 110.6}, {"goofy1", "goofy",
            110.6}, {"goofy2", "goofy", 110.6}, {"goofy2", "goofy", 110.6}, {"goofy3", "duck", 110.6}, {"goofy3",
            "duck", 110.6}, {"goofy4", "goofy", 110.6}, {"goofy4", "goofy", 110.6}, {"martian1", "mouse", 110.6}, {
          "martian1", "mouse", 110.6}, {"martian2", "mouse", 110.6}, {"martian2", "mouse", 110.6}, {"mickey1", "mouse",
            110.6}, {"mickey1", "mouse", 110.6}, {"minney2", "mouse", 110.6}, {"minney2", "mouse", 110.6}, {"pluto1",
            "dwag", 110.6}, {"pluto1", "dwag", 110.6}, {"pluto2", "dwag", 110.6}, {"pluto2", "dwag", 110.6}, {
          "zebra1", "zookeeper", 110.6}, {"zebra1", "zookeeper", 110.6}, {"zebra2", "zookeeper", 110.6}, {"zebra2",
            "zookeeper", 110.6}};
    checkResults("select fname, lname, avg(score) over() from testTable ORDER BY fname limit 100", expecteds1, true);

    Object[][] expecteds2 =
        {{"albert1", "albert", 25.475}, {"albert1", "albert", 25.475}, {"albert2", "albert", 25.475}, {"albert2",
            "albert", 25.475}, {"alice1", "wonderland", 25.475}, {"alice1", "wonderland", 25.475}, {"daffy1", "daffy",
            25.475}, {"daffy1", "daffy", 25.475}, {"daffy2", "duck", 25.475}, {"daffy2", "duck", 25.475}, {"daffy3",
            "duck", 25.475}, {"daffy3", "duck", 25.475}, {"donald1", "duck", 25.475}, {"donald1", "duck", 25.475},
            {"donald2", "duck", 25.475}, {"donald2", "duck", 25.475}, {"goofy1", "goofy", 25.475}, {"goofy1", "goofy",
            25.475}, {"goofy2", "goofy", 25.475}, {"goofy2", "goofy", 25.475}, {"goofy3", "duck", 25.475}, {"goofy3",
            "duck", 25.475}, {"goofy4", "goofy", 25.475}, {"goofy4", "goofy", 25.475}, {"martian1", "mouse",
            25.475}, {"martian1", "mouse", 25.475}, {"martian2", "mouse", 25.475}, {"martian2", "mouse", 25.475}, {
          "mickey1", "mouse", 25.475}, {"mickey1", "mouse", 25.475}, {"minney2", "mouse", 25.475}, {"minney2", "mouse",
            25.475}, {"pluto1", "dwag", 25.475}, {"pluto1", "dwag", 25.475}, {"pluto2", "dwag", 25.475}, {"pluto2",
            "dwag", 25.475}, {"zebra1", "zookeeper", 25.475}, {"zebra1", "zookeeper", 25.475}, {"zebra2", "zookeeper",
            25.475}, {"zebra2", "zookeeper", 25.475}};
    checkResults("select fname, lname, avg(money) over() from testTable ORDER BY fname limit 100", expecteds2, true);
  }

  @Test
  public void testBrokerMergeOrderByOneColumn() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0}, {"albert1", "albert", 160.0}, {"albert2", "albert", 160.0}, {"albert2",
            "albert", 160.0}, {"alice1", "wonderland", 104.0}, {"alice1", "wonderland", 104.0}, {"daffy1", "daffy",
            140.0}, {"daffy1", "daffy", 140.0}, {"daffy2", "duck", 98.75}, {"daffy2", "duck", 98.75}, {"daffy3",
            "duck", 98.75}, {"daffy3", "duck", 98.75}, {"donald1", "duck", 98.75}, {"donald1", "duck", 98.75}, {
            "donald2", "duck", 98.75}, {"donald2", "duck", 98.75}, {"goofy1", "goofy", 90.0}, {"goofy1", "goofy", 90.0},
            {"goofy2", "goofy", 90.0}, {"goofy2", "goofy", 90.0}, {"goofy3", "duck", 98.75}, {"goofy3", "duck",
            98.75}, {"goofy4", "goofy", 90.0}, {"goofy4", "goofy", 90.0}, {"martian1", "mouse", 101.29411764705883},
            {"martian1", "mouse", 101.29411764705883}, {"martian2", "mouse", 101.29411764705883}, {"martian2", "mouse",
            101.29411764705883}, {"mickey1", "mouse", 101.29411764705883}, {"mickey1", "mouse", 101.29411764705883},
            {"minney2", "mouse", 101.29411764705883}, {"minney2", "mouse", 101.29411764705883}, {"pluto1", "dwag",
            89.0}, {"pluto1", "dwag", 89.0}, {"pluto2", "dwag", 89.0}, {"pluto2", "dwag", 89.0}, {"zebra1",
            "zookeeper", 110.6}, {"zebra1", "zookeeper", 110.6}, {"zebra2", "zookeeper", 110.6}, {"zebra2",
            "zookeeper", 110.6}};
    checkResults("select fname, lname, avg(score) over(order by lname) from testTable ORDER BY fname limit 100",
        expecteds1, true);
  }

  @Test
  public void testBrokerMergeMultipleWindowFunctions() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 110.6, 25.475}, {"albert1", "albert", 110.6, 25.475}, {"albert2", "albert", 110.6,
            25.475}, {"albert2", "albert", 110.6, 25.475}, {"alice1", "wonderland", 110.6, 25.475}, {"alice1",
            "wonderland", 110.6, 25.475}, {"daffy1", "daffy", 110.6, 25.475}, {"daffy1", "daffy", 110.6, 25.475}, {
            "daffy2", "duck", 110.6, 25.475}, {"daffy2", "duck", 110.6, 25.475}, {"daffy3", "duck", 110.6, 25.475}, {
            "daffy3", "duck", 110.6, 25.475}, {"donald1", "duck", 110.6, 25.475}, {"donald1", "duck", 110.6, 25.475}, {
            "donald2", "duck", 110.6, 25.475}, {"donald2", "duck", 110.6, 25.475}, {"goofy1", "goofy", 110.6, 25.475},
            {"goofy1", "goofy", 110.6, 25.475}, {"goofy2", "goofy", 110.6, 25.475}, {"goofy2", "goofy", 110.6,
            25.475}, {"goofy3", "duck", 110.6, 25.475}, {"goofy3", "duck", 110.6, 25.475}, {"goofy4", "goofy", 110.6,
            25.475}, {"goofy4", "goofy", 110.6, 25.475}, {"martian1", "mouse", 110.6, 25.475}, {"martian1", "mouse",
            110.6, 25.475}, {"martian2", "mouse", 110.6, 25.475}, {"martian2", "mouse", 110.6, 25.475}, {"mickey1",
            "mouse", 110.6, 25.475}, {"mickey1", "mouse", 110.6, 25.475}, {"minney2", "mouse", 110.6, 25.475}, {
            "minney2", "mouse", 110.6, 25.475}, {"pluto1", "dwag", 110.6, 25.475}, {"pluto1", "dwag", 110.6, 25.475}, {
            "pluto2", "dwag", 110.6, 25.475}, {"pluto2", "dwag", 110.6, 25.475}, {"zebra1", "zookeeper", 110.6, 25.475},
            {"zebra1", "zookeeper", 110.6, 25.475}, {"zebra2", "zookeeper", 110.6, 25.475}, {"zebra2", "zookeeper",
            110.6, 25.475}};
    checkResults("select fname, lname, avg(score) over(), avg(money) over() from testTable ORDER BY fname limit 100",
        expecteds1, true);

    Object[][] expecteds2 =
        {{"albert1", "albert", 110.6, 160.0}, {"albert1", "albert", 110.6, 160.0}, {"albert2", "albert", 110.6,
            160.0}, {"albert2", "albert", 110.6, 160.0}, {"alice1", "wonderland", 110.6, 104.0}, {"alice1",
            "wonderland", 110.6, 104.0}, {"daffy1", "daffy", 110.6, 140.0}, {"daffy1", "daffy", 110.6, 140.0}, {
            "daffy2", "duck", 110.6, 98.75}, {"daffy2", "duck", 110.6, 98.75}, {"daffy3", "duck", 110.6, 98.75}, {
            "daffy3", "duck", 110.6, 98.75}, {"donald1", "duck", 110.6, 98.75}, {"donald1", "duck", 110.6, 98.75}, {
            "donald2", "duck", 110.6, 98.75}, {"donald2", "duck", 110.6, 98.75}, {"goofy1", "goofy", 110.6, 90.0}, {
            "goofy1", "goofy", 110.6, 90.0}, {"goofy2", "goofy", 110.6, 90.0}, {"goofy2", "goofy", 110.6, 90.0}, {
            "goofy3", "duck", 110.6, 98.75}, {"goofy3", "duck", 110.6, 98.75}, {"goofy4", "goofy", 110.6, 90.0}, {
            "goofy4", "goofy", 110.6, 90.0}, {"martian1", "mouse", 110.6, 101.29411764705883}, {"martian1", "mouse",
            110.6, 101.29411764705883}, {"martian2", "mouse", 110.6, 101.29411764705883}, {"martian2", "mouse", 110.6,
            101.29411764705883}, {"mickey1", "mouse", 110.6, 101.29411764705883}, {"mickey1", "mouse", 110.6,
            101.29411764705883}, {"minney2", "mouse", 110.6, 101.29411764705883}, {"minney2", "mouse", 110.6,
            101.29411764705883}, {"pluto1", "dwag", 110.6, 89.0}, {"pluto1", "dwag", 110.6, 89.0}, {"pluto2", "dwag",
            110.6, 89.0}, {"pluto2", "dwag", 110.6, 89.0}, {"zebra1", "zookeeper", 110.6, 110.6}, {"zebra1",
            "zookeeper", 110.6, 110.6}, {"zebra2", "zookeeper", 110.6, 110.6}, {"zebra2", "zookeeper", 110.6, 110.6}};
    checkResults("select fname, lname, avg(score) over(), avg(score) over(order by lname) from testTable "
            + "ORDER BY fname LIMIT 100",
        expecteds2, true);
  }

  @Test
  public void testBrokerMergeMultipleMixedWindowFunctions() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0, 25.475}, {"albert1", "albert", 160.0, 25.475}, {"albert2", "albert", 160.0,
            25.475}, {"albert2", "albert", 160.0, 25.475}, {"alice1", "wonderland", 104.0, 25.475}, {"alice1",
            "wonderland", 104.0, 25.475}, {"daffy1", "daffy", 140.0, 25.475}, {"daffy1", "daffy", 140.0, 25.475}, {
            "daffy2", "duck", 98.75, 25.475}, {"daffy2", "duck", 98.75, 25.475}, {"daffy3", "duck", 98.75, 25.475}, {
            "daffy3", "duck", 98.75, 25.475}, {"donald1", "duck", 98.75, 25.475}, {"donald1", "duck", 98.75, 25.475}, {
            "donald2", "duck", 98.75, 25.475}, {"donald2", "duck", 98.75, 25.475}, {"goofy1", "goofy", 90.0, 25.475}, {
            "goofy1", "goofy", 90.0, 25.475}, {"goofy2", "goofy", 90.0, 25.475}, {"goofy2", "goofy", 90.0, 25.475}, {
            "goofy3", "duck", 98.75, 25.475}, {"goofy3", "duck", 98.75, 25.475}, {"goofy4", "goofy", 90.0, 25.475}, {
            "goofy4", "goofy", 90.0, 25.475}, {"martian1", "mouse", 101.29411764705883, 25.475}, {"martian1", "mouse",
            101.29411764705883, 25.475}, {"martian2", "mouse", 101.29411764705883, 25.475}, {"martian2", "mouse",
            101.29411764705883, 25.475}, {"mickey1", "mouse", 101.29411764705883, 25.475}, {"mickey1", "mouse",
            101.29411764705883, 25.475}, {"minney2", "mouse", 101.29411764705883, 25.475}, {"minney2", "mouse",
            101.29411764705883, 25.475}, {"pluto1", "dwag", 89.0, 25.475}, {"pluto1", "dwag", 89.0, 25.475}, {"pluto2",
            "dwag", 89.0, 25.475}, {"pluto2", "dwag", 89.0, 25.475}, {"zebra1", "zookeeper", 110.6, 25.475}, {
            "zebra1", "zookeeper", 110.6, 25.475}, {"zebra2", "zookeeper", 110.6, 25.475}, {"zebra2", "zookeeper",
            110.6, 25.475}};
    checkResults("select fname, lname, avg(score) over(order by lname), avg(money) over() from testTable "
            + "ORDER BY fname LIMIT 100",
        expecteds1, true);
  }

  @Test
  public void testBrokerMergePartition() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 160.0}, {"albert1", "albert", 160.0}, {"albert2", "albert", 160.0}, {"albert2",
            "albert", 160.0}, {"alice1", "wonderland", 150.0}, {"alice1", "wonderland", 150.0}, {"daffy1", "daffy",
            100.0}, {"daffy1", "daffy", 100.0}, {"daffy2", "duck", 74.0}, {"daffy2", "duck", 74.0}, {"daffy3", "duck",
            74.0}, {"daffy3", "duck", 74.0}, {"donald1", "duck", 74.0}, {"donald1", "duck", 74.0}, {"donald2",
            "duck", 74.0}, {"donald2", "duck", 74.0}, {"goofy1", "goofy", 93.33333333333333}, {"goofy1", "goofy",
            93.33333333333333}, {"goofy2", "goofy", 93.33333333333333}, {"goofy2", "goofy", 93.33333333333333}, {
            "goofy3", "duck", 74.0}, {"goofy3", "duck", 74.0}, {"goofy4", "goofy", 93.33333333333333}, {"goofy4",
            "goofy", 93.33333333333333}, {"martian1", "mouse", 138.0}, {"martian1", "mouse", 138.0}, {"martian2",
            "mouse", 138.0}, {"martian2", "mouse", 138.0}, {"mickey1", "mouse", 138.0}, {"mickey1", "mouse", 138.0},
            {"minney2", "mouse", 138.0}, {"minney2", "mouse", 138.0}, {"pluto1", "dwag", 50.0}, {"pluto1", "dwag",
            50.0}, {"pluto2", "dwag", 50.0}, {"pluto2", "dwag", 50.0}, {"zebra1", "zookeeper", 170.0}, {"zebra1",
            "zookeeper", 170.0}, {"zebra2", "zookeeper", 170.0}, {"zebra2", "zookeeper", 170.0}};
    checkResults("select fname, lname, avg(score) over(partition by lname) from testTable ORDER BY fname limit 100",
        expecteds1, true);
  }

  @Test
  public void testBrokerMergePartitionWithOrder() {
    Object[][] expecteds1 =
        {{"albert1", "albert", 120.0}, {"albert1", "albert", 120.0}, {"albert2", "albert", 160.0}, {"albert2",
            "albert", 160.0}, {"alice1", "wonderland", 150.0}, {"alice1", "wonderland", 150.0}, {"daffy1", "daffy",
            100.0}, {"daffy1", "daffy", 100.0}, {"daffy2", "duck", 5.0}, {"daffy2", "duck", 5.0}, {"daffy3", "duck",
            7.5}, {"daffy3", "duck", 7.5}, {"donald1", "duck", 118.33333333333333}, {"donald1", "duck",
            118.33333333333333}, {"donald2", "duck", 91.25}, {"donald2", "duck", 91.25}, {"goofy1", "goofy", 110.0},
            {"goofy1", "goofy", 110.0}, {"goofy2", "goofy", 130.0}, {"goofy2", "goofy", 130.0}, {"goofy3", "duck",
            74.0}, {"goofy3", "duck", 74.0}, {"goofy4", "goofy", 93.33333333333333}, {"goofy4", "goofy",
            93.33333333333333}, {"martian1", "mouse", 250.0}, {"martian1", "mouse", 250.0}, {"martian2", "mouse",
            280.0}, {"martian2", "mouse", 280.0}, {"mickey1", "mouse", 197.33333333333334}, {"mickey1", "mouse",
            197.33333333333334}, {"minney2", "mouse", 138.0}, {"minney2", "mouse", 138.0}, {"pluto1", "dwag", 120.0},
            {"pluto1", "dwag", 120.0}, {"pluto2", "dwag", 50.0}, {"pluto2", "dwag", 50.0}, {"zebra1", "zookeeper",
            120.0}, {"zebra1", "zookeeper", 120.0}, {"zebra2", "zookeeper", 170.0}, {"zebra2", "zookeeper", 170.0}};
    checkResults(
        "select fname, lname, avg(score) over(partition by lname order by fname) from testTable ORDER BY fname limit "
            + "100", expecteds1, true);

    Object[][] expecteds2 =
        {{"albert1", "albert", 120.0}, {"albert1", "albert", 120.0}, {"albert2", "albert", 160.0}, {"albert2",
            "albert", 160.0}, {"alice1", "wonderland", 150.0}, {"alice1", "wonderland", 150.0}, {"daffy1", "daffy",
            100.0}};
    checkResults(
        "select fname, lname, avg(score) over(partition by lname order by fname) from testTable ORDER BY fname ASC "
            + "limit 7",
        expecteds2, true);

    Object[][] expecteds3 =
        {{"zebra2", "zookeeper", 170.0}, {"zebra2", "zookeeper", 170.0}, {"zebra1", "zookeeper", 120.0}, {"zebra1",
            "zookeeper", 120.0}, {"pluto2", "dwag", 50.0}, {"pluto2", "dwag", 50.0}, {"pluto1", "dwag", 120.0}};
    checkResults(
        "select fname, lname, avg(score) over(partition by lname order by fname) from testTable ORDER BY fname DESC "
            + "limit 7",
        expecteds3, true);
  }

  @Test
  public void testCountWindowFunction() {
    Object[][] expecteds1 = {{20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0},
        {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}, {20.0}};
    checkResults("SELECT count(*) over() FROM testTable LIMIT 100", expecteds1, false);

    Object[][] expecteds2 = {{"albert", 20.0}, {"albert", 20.0}, {"daffy", 20.0}, {"duck", 20.0}, {"duck", 20.0},
        {"duck", 20.0}, {"duck", 20.0}, {"duck", 20.0}, {"dwag", 20.0}, {"dwag", 20.0}, {"goofy", 20.0},
        {"goofy", 20.0}, {"goofy", 20.0}, {"mouse", 20.0}, {"mouse", 20.0}, {"mouse", 20.0}, {"mouse", 20.0},
        {"wonderland", 20.0}, {"zookeeper", 20.0}, {"zookeeper", 20.0}};
    checkResults("SELECT lname, count(*) over() FROM testTable ORDER BY lname LIMIT 100", expecteds2, false);

    Object[][] expecteds3 = {{"albert", 2.0}, {"albert", 2.0}, {"daffy", 1.0}, {"duck", 5.0}, {"duck", 5.0},
        {"duck", 5.0}, {"duck", 5.0}, {"duck", 5.0}, {"dwag", 2.0}, {"dwag", 2.0}, {"goofy", 3.0}, {"goofy", 3.0},
        {"goofy", 3.0}, {"mouse", 4.0}, {"mouse", 4.0}, {"mouse", 4.0}, {"mouse", 4.0}, {"wonderland", 1.0},
        {"zookeeper", 2.0}, {"zookeeper", 2.0}};
    checkResults("SELECT lname, count(*) over(partition by lname) FROM testTable ORDER BY lname, fname LIMIT 100",
        expecteds3, false);

    Object[][] expecteds4 = {{"albert", "albert1", 1.0}, {"albert", "albert2", 2.0}, {"daffy", "daffy1", 1.0},
        {"duck", "daffy2", 1.0}, {"duck", "daffy3", 2.0}, {"duck", "donald1", 3.0}, {"duck", "donald2", 4.0},
        {"duck", "goofy3", 5.0}, {"dwag", "pluto1", 1.0}, {"dwag", "pluto2", 2.0}, {"goofy", "goofy1", 1.0},
        {"goofy", "goofy2", 2.0}, {"goofy", "goofy4", 3.0}, {"mouse", "martian1", 1.0}, {"mouse", "martian2", 2.0},
        {"mouse", "mickey1", 3.0}, {"mouse", "minney2", 4.0}, {"wonderland", "alice1", 1.0},
        {"zookeeper", "zebra1", 1.0}, {"zookeeper", "zebra2", 2.0}};
    checkResults(
        "SELECT lname, fname, count(*) over(partition by lname order by fname) FROM testTable "
        + "ORDER BY lname, fname LIMIT 100",
        expecteds4, false);
  }

  @Test
  public void testMinWindowFunction() {
    Object[][] expecteds1 = {{-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0},
        {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}, {-40.0}};
    checkResults("SELECT min(score) over() FROM testTable LIMIT 100", expecteds1, false);

    Object[][] expecteds2 = {{"albert", -40.0}, {"albert", -40.0}, {"daffy", -40.0}, {"duck", -40.0}, {"duck", -40.0},
        {"duck", -40.0}, {"duck", -40.0}, {"duck", -40.0}, {"dwag", -40.0}, {"dwag", -40.0}, {"goofy", -40.0},
        {"goofy", -40.0}, {"goofy", -40.0}, {"mouse", -40.0}, {"mouse", -40.0}, {"mouse", -40.0}, {"mouse", -40.0},
        {"wonderland", -40.0}, {"zookeeper", -40.0}, {"zookeeper", -40.0}};
    checkResults("SELECT lname, min(score) over() FROM testTable ORDER BY lname LIMIT 100", expecteds2, false);

    Object[][] expecteds3 = {{"albert", 120.0}, {"albert", 120.0}, {"daffy", 100.0}, {"duck", 5.0}, {"duck", 5.0},
        {"duck", 5.0}, {"duck", 5.0}, {"duck", 5.0}, {"dwag", -20.0}, {"dwag", -20.0}, {"goofy", 20.0},
        {"goofy", 20.0}, {"goofy", 20.0}, {"mouse", -40.0}, {"mouse", -40.0}, {"mouse", -40.0}, {"mouse", -40.0},
        {"wonderland", 150.0}, {"zookeeper", 120.0}, {"zookeeper", 120.0}};
    checkResults("SELECT lname, min(score) over(partition by lname) FROM testTable ORDER BY lname, fname LIMIT 100",
        expecteds3, false);

    Object[][] expecteds4 = {{"albert", "albert1", 120.0}, {"albert", "albert2", 120.0}, {"daffy", "daffy1", 100.0},
        {"duck", "daffy2", 5.0}, {"duck", "daffy3", 5.0}, {"duck", "donald1", 5.0}, {"duck", "donald2", 5.0},
        {"duck", "goofy3", 5.0}, {"dwag", "pluto1", 120.0}, {"dwag", "pluto2", -20.0}, {"goofy", "goofy1", 110.0},
        {"goofy", "goofy2", 110.0}, {"goofy", "goofy4", 20.0}, {"mouse", "martian1", 250.0},
        {"mouse", "martian2", 250.0}, {"mouse", "mickey1", 32.0}, {"mouse", "minney2", -40.0},
        {"wonderland", "alice1", 150.0}, {"zookeeper", "zebra1", 120.0}, {"zookeeper", "zebra2", 120.0}};
    checkResults(
        "SELECT lname, fname, min(score) over(partition by lname order by fname) FROM testTable "
            + "ORDER BY lname, fname LIMIT 100",
        expecteds4, false);
  }

  @Test
  public void testMaxWindowFunction() {
    Object[][] expecteds1 = {{340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0},
        {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}, {340.0}};
    checkResults("SELECT max(score) over() FROM testTable LIMIT 100", expecteds1, false);

    Object[][] expecteds2 = {{"albert", 340.0}, {"albert", 340.0}, {"daffy", 340.0}, {"duck", 340.0}, {"duck", 340.0},
        {"duck", 340.0}, {"duck", 340.0}, {"duck", 340.0}, {"dwag", 340.0}, {"dwag", 340.0}, {"goofy", 340.0},
        {"goofy", 340.0}, {"goofy", 340.0}, {"mouse", 340.0}, {"mouse", 340.0}, {"mouse", 340.0}, {"mouse", 340.0},
        {"wonderland", 340.0}, {"zookeeper", 340.0}, {"zookeeper", 340.0}};
    checkResults("SELECT lname, max(score) over() FROM testTable ORDER BY lname LIMIT 100", expecteds2, false);

    Object[][] expecteds3 = {{"albert", 200.0}, {"albert", 200.0}, {"daffy", 100.0}, {"duck", 340.0}, {"duck", 340.0},
        {"duck", 340.0}, {"duck", 340.0}, {"duck", 340.0}, {"dwag", 120.0}, {"dwag", 120.0}, {"goofy", 150.0},
        {"goofy", 150.0}, {"goofy", 150.0}, {"mouse", 310.0}, {"mouse", 310.0}, {"mouse", 310.0}, {"mouse", 310.0},
        {"wonderland", 150.0}, {"zookeeper", 220.0}, {"zookeeper", 220.0}};
    checkResults("SELECT lname, max(score) over(partition by lname) FROM testTable ORDER BY lname, fname LIMIT 100",
        expecteds3, false);

    Object[][] expecteds4 = {{"albert", "albert1", 120.0}, {"albert", "albert2", 200.0}, {"daffy", "daffy1", 100.0},
        {"duck", "daffy2", 5.0}, {"duck", "daffy3", 10.0}, {"duck", "donald1", 340.0}, {"duck", "donald2", 340.0},
        {"duck", "goofy3", 340.0}, {"dwag", "pluto1", 120.0}, {"dwag", "pluto2", 120.0}, {"goofy", "goofy1", 110.0},
        {"goofy", "goofy2", 150.0}, {"goofy", "goofy4", 150.0}, {"mouse", "martian1", 250.0},
        {"mouse", "martian2", 310.0}, {"mouse", "mickey1", 310.0}, {"mouse", "minney2", 310.0},
        {"wonderland", "alice1", 150.0}, {"zookeeper", "zebra1", 120.0}, {"zookeeper", "zebra2", 220.0}};
    checkResults(
        "SELECT lname, fname, max(score) over(partition by lname order by fname) FROM testTable "
            + "ORDER BY lname, fname LIMIT 100",
        expecteds4, false);
  }

  @Test
  public void testSumWindowFunction() {
    Object[][] expecteds1 = {{2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0},
        {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}, {2212.0}};
    checkResults("SELECT sum(score) over() FROM testTable LIMIT 100", expecteds1, false);

    Object[][] expecteds2 = {{"albert", 2212.0}, {"albert", 2212.0}, {"daffy", 2212.0}, {"duck", 2212.0},
        {"duck", 2212.0}, {"duck", 2212.0}, {"duck", 2212.0}, {"duck", 2212.0}, {"dwag", 2212.0}, {"dwag", 2212.0},
        {"goofy", 2212.0}, {"goofy", 2212.0}, {"goofy", 2212.0}, {"mouse", 2212.0}, {"mouse", 2212.0},
        {"mouse", 2212.0}, {"mouse", 2212.0}, {"wonderland", 2212.0}, {"zookeeper", 2212.0}, {"zookeeper", 2212.0}};
    checkResults("SELECT lname, sum(score) over() FROM testTable ORDER BY lname LIMIT 100", expecteds2, false);

    Object[][] expecteds3 = {{"albert", 320.0}, {"albert", 320.0}, {"daffy", 100.0}, {"duck", 370.0}, {"duck", 370.0},
        {"duck", 370.0}, {"duck", 370.0}, {"duck", 370.0}, {"dwag", 100.0}, {"dwag", 100.0}, {"goofy", 280.0},
        {"goofy", 280.0}, {"goofy", 280.0}, {"mouse", 552.0}, {"mouse", 552.0}, {"mouse", 552.0}, {"mouse", 552.0},
        {"wonderland", 150.0}, {"zookeeper", 340.0}, {"zookeeper", 340.0}};
    checkResults("SELECT lname, sum(score) over(partition by lname) FROM testTable ORDER BY lname, fname LIMIT 100",
        expecteds3, false);

    Object[][] expecteds4 = {{"albert", "albert1", 120.0}, {"albert", "albert2", 320.0}, {"daffy", "daffy1", 100.0},
        {"duck", "daffy2", 5.0}, {"duck", "daffy3", 15.0}, {"duck", "donald1", 355.0}, {"duck", "donald2", 365.0},
        {"duck", "goofy3", 370.0}, {"dwag", "pluto1", 120.0}, {"dwag", "pluto2", 100.0}, {"goofy", "goofy1", 110.0},
        {"goofy", "goofy2", 260.0}, {"goofy", "goofy4", 280.0}, {"mouse", "martian1", 250.0},
        {"mouse", "martian2", 560.0}, {"mouse", "mickey1", 592.0}, {"mouse", "minney2", 552.0},
        {"wonderland", "alice1", 150.0}, {"zookeeper", "zebra1", 120.0}, {"zookeeper", "zebra2", 340.0}};
    checkResults(
        "SELECT lname, fname, sum(score) over(partition by lname order by fname) FROM testTable "
            + "ORDER BY lname, fname LIMIT 100",
        expecteds4, false);
  }

  @Test
  public void testOuterOrderByNonWindowColumn() {
    Object[][] expecteds1 =
        {{-44.5f, "mouse", "martian2", 2.0}, {-28.0f, "daffy", "daffy1", 1.0}, {-16.0f, "dwag", "pluto1", 1.0},
            {-16.0f, "zookeeper", "zebra1", 1.0}, {-16.0f, "zookeeper", "zebra2", 2.0},
            {1.5f, "albert", "albert2", 2.0}, {2.5f, "dwag", "pluto2", 2.0}, {4.5f, "duck", "daffy2", 1.0},
            {4.5f, "duck", "donald2", 4.0}, {7.5f, "duck", "goofy3", 5.0}, {10.0f, "mouse", "mickey1", 3.0},
            {10.5f, "wonderland", "alice1", 1.0}, {11.5f, "duck", "donald1", 3.0}, {12.0f, "goofy", "goofy2", 2.0},
            {16.0f, "goofy", "goofy1", 1.0}, {20.5f, "goofy", "goofy4", 3.0}, {32.5f, "mouse", "martian1", 1.0},
            {46.0f, "duck", "daffy3", 2.0}, {200.5f, "albert", "albert1", 1.0}, {250.0f, "mouse", "minney2", 4.0}};
    checkResults(
        "SELECT lname, fname, money, count(*) OVER(PARTITION by lname ORDER by fname) FROM testTable "
        + " ORDER BY money LIMIT 100",
        expecteds1, false);
  }

  @Test(expectedExceptions = {IllegalStateException.class}, expectedExceptionsMessageRegExp = "Window functions can"
      + " not be used with GROUP BY clause.")
  public void testWindowFunctionWithGroupBy() {
    checkResults(
        "select fname, lname, avg(score) over(partition by lname order by fname) from testTable GROUP BY 1,2,3 "
            + "ORDER BY fname ASC LIMIT 7", new Object[][]{{}}, true);
  }

  @Test(expectedExceptions = {IllegalStateException.class}, expectedExceptionsMessageRegExp = "PARTITION BY clause"
      + " must be the same across all window functions in the select list")
  public void testWindowFunctionsWithDifferentPartitionClause() {
    Object[][] expecteds1 = {{}};
    checkResults(
        "select avg(score) over(partition by lname), avg(score) over(partition by fname) from testTable ORDER BY fname",
        expecteds1, false);
  }

  @Test(expectedExceptions = {IllegalStateException.class}, expectedExceptionsMessageRegExp = "ORDER BY clause must "
      + "be same across all window functions in the select list")
  public void testWindowFunctionsWithDifferentOrderClause() {
    Object[][] expecteds1 = {{}};
    checkResults(
        "select avg(score) over(order by lname), avg(score) over(order by fname) from testTable ORDER BY fname",
        expecteds1, false);
  }

  /** Sanity check to make sure window function implementation doesn't break anything other queries. */
  @Test
  public void testNonWindowFunctionQuery() {
    //checkResults("select lname, sum(score) from testTable GROUP BY lname", new Object[][]{{}}, true);
    checkResults("SELECT lname, fname FROM testTable ORDER BY lname, fname",
        new Object[][]{{"albert", "albert1"}, {"albert", "albert1"}, {"albert", "albert2"}, {"albert", "albert2"},
            {"daffy", "daffy1"}, {"daffy", "daffy1"}, {"duck", "daffy2"}, {"duck", "daffy2"}, {"duck", "daffy3"},
            {"duck", "daffy3"}}, true);
  }

  private static Map asMap(Object[][] expecteds, int uniqueKeyIndex) {
    Map<Object, Object[]> map = new HashMap();
    for (Object[] expected : expecteds) {
      map.put(expected[uniqueKeyIndex], expected);
    }
    return map;
  }

  protected void checkResults(String query, Object[][] expecteds, boolean mimicRealtimeServer) {
    BrokerResponseNative response = getBrokerResponse(query, mimicRealtimeServer);

    List<Object[]> rows = response.getResultTable().getRows();
    Assert.assertEquals(rows.size(), expecteds.length);

    for (int i = 0; i < rows.size(); i++) {
      Object[] actual = rows.get(i);
      //print(actual);
      Assert.assertEquals(actual, expecteds[i]);
    }
    System.out.println("");
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
