/** Copyright 2013 BlackBerry, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 */

package com.rim.logdriver.sawmill;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionLogger {
  private static final Logger LOG = LoggerFactory
      .getLogger(ExceptionLogger.class);

  private final Object lock = new Object();

  private long reportPeriod = 10000;
  private Throwable lastT = null;
  private int repeatCount = 0;

  private ScheduledExecutorService executor;

  private final RepeatTracker repeatTracker;

  private static class SingletonHolder {
    public static final ExceptionLogger INSTANCE = new ExceptionLogger();
  }

  public static ExceptionLogger getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private ExceptionLogger() {
    executor = Executors.newScheduledThreadPool(1);
    repeatTracker = new RepeatTracker();

    executor.scheduleWithFixedDelay(repeatTracker, reportPeriod, reportPeriod,
        TimeUnit.MILLISECONDS);
  }

  public void logException(Throwable t, String info) {
    synchronized (lock) {
      if (lastT != null && t.getMessage().equals(lastT.getMessage())) {
        repeatCount++;
      } else {
        repeatTracker.report();

        if (info != null) {
          LOG.warn("Exception caught. {}", info, t);
        } else {
          LOG.warn("Exception caught.", t);
        }

        lastT = t;
      }
    }
  }

  private class RepeatTracker implements Runnable {
    @Override
    public void run() {
      report();
    }

    public void report() {
      synchronized (lock) {
        if (repeatCount > 0) {
          LOG.warn("Last message repeated {} times.", repeatCount);

          lastT = null;
          repeatCount = 0;
        }
      }
    }
  }
}
