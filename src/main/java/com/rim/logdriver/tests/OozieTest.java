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

package com.rim.logdriver.tests;

import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;

import com.rim.logdriver.oozie.SynchronizedOozieClient;

public class OozieTest {

  public static void main(String[] args) {
    new OozieTest().run(args);
  }

  public void run(String[] args) {
    String jobId = args[0];
    String oozieUrl = System.getenv("OOZIE_URL");

    {
      System.out.println("One Client");
      OozieClient oozieClient = new SynchronizedOozieClient(
          new AuthOozieClient(oozieUrl));
      Thread[] threads = new Thread[100];
      for (int i = 0; i < 100; i++) {
        threads[i] = new Thread(new Runner(jobId, oozieClient));
      }

      for (int i = 0; i < 100; i++) {
        threads[i].start();
      }
      for (int i = 0; i < 100; i++) {
        try {
          threads[i].join(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    {
      System.out.println("Many Clients");
      Thread[] threads = new Thread[100];
      for (int i = 0; i < 100; i++) {
        OozieClient oozieClient = new SynchronizedOozieClient(
            new AuthOozieClient(oozieUrl));
        threads[i] = new Thread(new Runner(jobId, oozieClient));
      }

      for (int i = 0; i < 100; i++) {
        threads[i].start();
      }
      for (int i = 0; i < 100; i++) {
        try {
          threads[i].join(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

  }

  private class Runner implements Runnable {
    private final String jobId;
    private final OozieClient oozieClient;

    public Runner(String jobId, OozieClient oozieClient) {
      this.jobId = jobId;
      this.oozieClient = oozieClient;
    }

    @Override
    public void run() {
      try {
        System.out
            .println(oozieClient.getJobInfo(jobId).getStatus().toString());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }
}
