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

/**
 * Wrapper to OozieClient that makes all non-static methods synchronized.
 * 
 * Targets Oozie 2.3.2, but methods for Oozie 3.3.0 are simply commented out.
 */
package com.rim.logdriver.oozie;

import java.io.IOException;
import java.io.OutputStream;
//import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import org.apache.oozie.client.BulkResponse;
//import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

public class SynchronizedOozieClient extends OozieClient {
  private OozieClient client = null;
  private Object lock = new Object();

  public SynchronizedOozieClient(OozieClient client) {
    this.client = client;
  }

  @Override
  public String getOozieUrl() {
    synchronized (lock) {
      return client.getOozieUrl();
    }
  }

  @Override
  public String getProtocolUrl() throws OozieClientException {
    synchronized (lock) {
      return client.getProtocolUrl();
    }
  }

  @Override
  public int getDebugMode() {
    synchronized (lock) {
      return client.getDebugMode();
    }
  }

  @Override
  public void setDebugMode(int debugMode) {
    synchronized (lock) {
      client.setDebugMode(debugMode);
    }
  }

  @Override
  public synchronized void validateWSVersion() throws OozieClientException {
    synchronized (lock) {
      client.validateWSVersion();
    }
  }

  @Override
  public Properties createConfiguration() {
    synchronized (lock) {
      return client.createConfiguration();
    }
  }

  @Override
  public void setHeader(String name, String value) {
    synchronized (lock) {
      client.setHeader(name, value);
    }
  }

  @Override
  public String getHeader(String name) {
    synchronized (lock) {
      return client.getHeader(name);
    }
  }

  @Override
  public Map<String, String> getHeaders() {
    synchronized (lock) {
      return client.getHeaders();
    }
  }

  @Override
  public void removeHeader(String name) {
    synchronized (lock) {
      client.removeHeader(name);
    }
  }

  @Override
  public Iterator<String> getHeaderNames() {
    synchronized (lock) {
      return client.getHeaderNames();
    }
  }

  @Override
  public void writeToXml(Properties props, OutputStream out) throws IOException {
    synchronized (lock) {
      client.writeToXml(props, out);
    }
  }

  @Override
  public String submit(Properties conf) throws OozieClientException {
    synchronized (lock) {
      return client.submit(conf);
    }
  }

  @Override
  public String dryrun(Properties conf) throws OozieClientException {
    synchronized (lock) {
      return client.dryrun(conf);
    }
  }

  @Override
  public void start(String jobId) throws OozieClientException {
    synchronized (lock) {
      client.start(jobId);
    }
  }

  @Override
  public String run(Properties conf) throws OozieClientException {
    synchronized (lock) {
      return client.run(conf);
    }
  }

  @Override
  public void reRun(String jobId, Properties conf) throws OozieClientException {
    synchronized (lock) {
      client.reRun(jobId, conf);
    }
  }

  @Override
  public void suspend(String jobId) throws OozieClientException {
    synchronized (lock) {
      client.suspend(jobId);
    }
  }

  @Override
  public void resume(String jobId) throws OozieClientException {
    synchronized (lock) {
      client.resume(jobId);
    }
  }

  @Override
  public void kill(String jobId) throws OozieClientException {
    synchronized (lock) {
      client.kill(jobId);
    }
  }

  @Override
  public void change(String jobId, String changeValue)
      throws OozieClientException {
    synchronized (lock) {
      client.change(jobId, changeValue);
    }
  }

  @Override
  public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
    synchronized (lock) {
      return client.getJobInfo(jobId);
    }
  }

  @Override
  public WorkflowJob getJobInfo(String jobId, int start, int len)
      throws OozieClientException {
    synchronized (lock) {
      return client.getJobInfo(jobId, start, len);
    }
  }

  @Override
  public WorkflowAction getWorkflowActionInfo(String actionId)
      throws OozieClientException {
    synchronized (lock) {
      return client.getWorkflowActionInfo(actionId);
    }
  }

  @Override
  public String getJobLog(String jobId) throws OozieClientException {
    synchronized (lock) {
      return client.getJobLog(jobId);
    }
  }

  // @Override
  // public void getJobLog(String jobId, String logRetrievalType,
  // String logRetrievalScope, PrintStream ps) throws OozieClientException {
  // synchronized (lock) {
  // client.getJobLog(jobId, logRetrievalType, logRetrievalScope, ps);
  // }
  // }

  @Override
  public String getJobDefinition(String jobId) throws OozieClientException {
    synchronized (lock) {
      return client.getJobDefinition(jobId);
    }
  }

  // @Override
  // public BundleJob getBundleJobInfo(String jobId) throws OozieClientException
  // {
  // synchronized (lock) {
  // return client.getBundleJobInfo(jobId);
  // }
  // }

  @Override
  public CoordinatorJob getCoordJobInfo(String jobId)
      throws OozieClientException {
    synchronized (lock) {
      return client.getCoordJobInfo(jobId);
    }
  }

  // @Override
  // public CoordinatorJob getCoordJobInfo(String jobId, String filter, int
  // start,
  // int len) throws OozieClientException {
  // synchronized (lock) {
  // return client.getCoordJobInfo(jobId, filter, start, len);
  // }
  // }

  @Override
  public CoordinatorAction getCoordActionInfo(String actionId)
      throws OozieClientException {
    synchronized (lock) {
      return client.getCoordActionInfo(actionId);
    }
  }

  @Override
  public List<CoordinatorAction> reRunCoord(String jobId, String rerunType,
      String scope, boolean refresh, boolean noCleanup)
      throws OozieClientException {
    synchronized (lock) {
      return client.reRunCoord(jobId, rerunType, scope, refresh, noCleanup);
    }
  }

  // @Override
  // public Void reRunBundle(String jobId, String coordScope, String dateScope,
  // boolean refresh, boolean noCleanup) throws OozieClientException {
  // synchronized (lock) {
  // return client.reRunBundle(jobId, coordScope, dateScope, refresh,
  // noCleanup);
  // }
  // }

  @Override
  public List<WorkflowJob> getJobsInfo(String filter, int start, int len)
      throws OozieClientException {
    synchronized (lock) {
      return client.getJobsInfo(filter, start, len);
    }
  }

  @Override
  public List<WorkflowJob> getJobsInfo(String filter)
      throws OozieClientException {
    synchronized (lock) {
      return client.getJobsInfo(filter);
    }
  }

  @Override
  public void getSlaInfo(int start, int len) throws OozieClientException {
    synchronized (lock) {
      client.getSlaInfo(start, len);
    }
  }

  @Override
  public String getJobId(String externalId) throws OozieClientException {
    synchronized (lock) {
      return client.getJobId(externalId);
    }
  }

  @Override
  public void setSystemMode(SYSTEM_MODE status) throws OozieClientException {
    synchronized (lock) {
      client.setSystemMode(status);
    }
  }

  @Override
  public SYSTEM_MODE getSystemMode() throws OozieClientException {
    synchronized (lock) {
      return client.getSystemMode();
    }
  }

  @Override
  public String getServerBuildVersion() throws OozieClientException {
    synchronized (lock) {
      return client.getServerBuildVersion();
    }
  }

  @Override
  public String getClientBuildVersion() {
    synchronized (lock) {
      return client.getClientBuildVersion();
    }
  }

  @Override
  public List<CoordinatorJob> getCoordJobsInfo(String filter, int start, int len)
      throws OozieClientException {
    synchronized (lock) {
      return client.getCoordJobsInfo(filter, start, len);
    }
  }

  // @Override
  // public List<BundleJob> getBundleJobsInfo(String filter, int start, int len)
  // throws OozieClientException {
  // synchronized (lock) {
  // return client.getBundleJobsInfo(filter, start, len);
  // }
  // }

  // @Override
  // public List<BulkResponse> getBulkInfo(String filter, int start, int len)
  // throws OozieClientException {
  // synchronized (lock) {
  // return client.getBulkInfo(filter, start, len);
  // }
  // }

  @Override
  public List<String> getQueueDump() throws OozieClientException {
    synchronized (lock) {
      return client.getQueueDump();
    }
  }
}
