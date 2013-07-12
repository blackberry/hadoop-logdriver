package com.rim.logdriver.filemanager;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rim.logdriver.fs.FileManager;
import com.rim.logdriver.test.util.LocalZkServer;

public class GetHoursForTimeRangeTest {

  private LocalZkServer zkServer;
  private Configuration conf;

  @Before
  public void setup() throws Exception {
    zkServer = new LocalZkServer();

    conf = new Configuration();
    conf.set("zk.connect.string", "localhost:" + zkServer.getClientport());
  }

  @Test
  public void testTimeRanges() throws Exception {

    FileManager fm = new FileManager(conf);

    assertEquals(1, fm.getHoursForTimeRange(0, 0).size());
    assertEquals(1, fm.getHoursForTimeRange(0, 1).size());
    assertEquals(1, fm.getHoursForTimeRange(0, 3600000).size());

    assertEquals(2, fm.getHoursForTimeRange(0, 3600001).size());
    assertEquals(2, fm.getHoursForTimeRange(0, 7200000).size());

    assertEquals(3, fm.getHoursForTimeRange(0, 7200001).size());

    assertEquals(1, fm.getHoursForTimeRange(1, 1800000).size());
    assertEquals(1, fm.getHoursForTimeRange(1, 3600000).size());

    assertEquals(2, fm.getHoursForTimeRange(1, 3600001).size());
    assertEquals(2, fm.getHoursForTimeRange(1, 7200000).size());

    assertEquals(3, fm.getHoursForTimeRange(1, 7200001).size());

    assertEquals(1, fm.getHoursForTimeRange(1800000, 1800000).size());
    assertEquals(1, fm.getHoursForTimeRange(1800000, 3600000).size());

    assertEquals(2, fm.getHoursForTimeRange(1800000, 3600001).size());
    assertEquals(2, fm.getHoursForTimeRange(1800000, 7200000).size());

    assertEquals(3, fm.getHoursForTimeRange(1800000, 7200001).size());

  }

  @After
  public void cleanup() throws Exception {
    zkServer.shutdown();
  }
}