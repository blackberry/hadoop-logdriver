package com.rim.logdriver.pig;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class DateFormatterTest {

  @Test
  public void TestBadInputClass() throws IOException {
    Tuple t = TupleFactory.getInstance().newTuple();
    t.append(new Integer(1));

    DateFormatter formatter = new DateFormatter("RFC5424");

    String date = formatter.exec(t);
    Assert.assertNull(date);
  }
}
