package com.rim.logdriver.filemanager;

import static org.junit.Assert.*;

import com.rim.logdriver.locks.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.rim.logdriver.fs.FileManager;

public class GetHoursForTimeRangeTest {

  LockUtilsTest lut = new LockUtilsTest();
  
  @Before
  public void setup() throws Exception {
    lut.setup();
  }
  
  @Test
  public void testTimeRanges() throws Exception {
 
    FileManager fm = new FileManager(lut.getConf());
  
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
}