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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiEnqueueHandler implements IoHandler, MultiEnqueueHandlerMBean {
  private static final Logger LOG = LoggerFactory
      .getLogger(MultiEnqueueHandler.class);

  private final BlockingQueue<String>[] queues;
  private final int numBuckets;
  private final ConcurrentMap<String, BucketInfo> bucketMap = new ConcurrentHashMap<String, BucketInfo>();
  private final Object bucketLock = new Object();

  private long expiryCheckPeriod = 600000; // 10 minutes
  private long timeToLive = 3600000; // 1 hour
  private long nextExpiryCheck = System.currentTimeMillis() + expiryCheckPeriod;

  private long enqueued = 0;
  private long dropped = 0;

  public MultiEnqueueHandler(BlockingQueue<String>[] queues) {
    this.queues = queues;
    numBuckets = queues.length;
  }

  @Override
  public void exceptionCaught(IoSession session, Throwable cause)
      throws Exception {
    ExceptionLogger.getInstance().logException(cause, "In session " + session);
  }

  @Override
  public void messageReceived(IoSession session, Object message)
      throws Exception {
    LOG.debug("Received message from session {}:{}", session, message);

    long now = System.currentTimeMillis();

    String m = message.toString();
    // Hash the hostname to determine the queue to go on. The hostname is always
    // what's in between the first two spaces.
    // (This is not true if we start using version numbers in syslog!)
    String hostname = "";
    int firstSpace = m.indexOf(' ');
    if (firstSpace > -1) {
      int secondSpace = m.indexOf(' ', firstSpace + 1);
      if (secondSpace > -1) {
        hostname = m.substring(firstSpace + 1, secondSpace);
      }
    }

    BucketInfo bucket = bucketMap.get(hostname);
    if (bucket == null) {
      synchronized (bucketLock) {
        bucket = bucketMap.get(hostname);
        if (bucket == null) {
          // need to find a new bucket. So check how many are in each bucket,
          // and pick the least loaded one.
          int[] counts = new int[numBuckets];
          for (int i = 0; i < numBuckets; i++) {
            counts[i] = 0;
          }
          for (BucketInfo val : bucketMap.values()) {
            counts[val.bucket]++;
          }

          int smallestBucket = 0;
          int smallestCount = Integer.MAX_VALUE;
          for (int i = 0; i < numBuckets; i++) {
            if (counts[i] < smallestCount) {
              smallestBucket = i;
              smallestCount = counts[i];
            }
          }

          bucket = new BucketInfo();
          bucket.bucket = smallestBucket;
          bucketMap.put(hostname, bucket);
        }
      }
    }

    bucket.lastUsed = now;

    BlockingQueue<String> queue = queues[bucket.bucket];

    if (queue.offer(m)) {
      ++enqueued;
    } else {
      ++dropped;
    }

    // Clear out old entries periodically, to prevent a memory leak.
    if (now >= nextExpiryCheck) {
      long expiryTime = now - timeToLive;
      for (Entry<String, BucketInfo> e : new HashSet<Entry<String, BucketInfo>>(
          bucketMap.entrySet())) {
        if (e.getValue().lastUsed <= expiryTime) {
          bucketMap.remove(e.getKey());
        }
      }
    }
  }

  @Override
  public void messageSent(IoSession session, Object message) throws Exception {
    LOG.debug("Sent message to session {}:{}", session, message);
  }

  @Override
  public void sessionClosed(IoSession session) throws Exception {
    LOG.info("Session closed: {}", session);
  }

  @Override
  public void sessionCreated(IoSession session) throws Exception {
    LOG.info("Session created: {}", session);
  }

  @Override
  public void sessionIdle(IoSession session, IdleStatus status)
      throws Exception {
    LOG.debug("Session idle: {} [{}]", session, status);
  }

  @Override
  public void sessionOpened(IoSession session) throws Exception {
    LOG.info("Session opened: {}", session);
  }

  @Override
  public long getEnqueued() {
    return enqueued;
  }

  @Override
  public long getDropped() {
    return dropped;
  }

  @Override
  public long getTotalIncoming() {
    return enqueued + dropped;
  }

  private class BucketInfo {
    int bucket;
    long lastUsed;
  }

}
