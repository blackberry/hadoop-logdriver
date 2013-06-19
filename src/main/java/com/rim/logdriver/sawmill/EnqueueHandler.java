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

import java.util.concurrent.BlockingQueue;

import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnqueueHandler implements IoHandler, EnqueueHandlerMBean {
  private static final Logger LOG = LoggerFactory
      .getLogger(EnqueueHandler.class);

  private final BlockingQueue<String> queue;

  private long enqueued = 0;
  private long dropped = 0;

  public EnqueueHandler(BlockingQueue<String> queue) {
    this.queue = queue;
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

    if (queue.offer(message.toString())) {
      ++enqueued;
    } else {
      ++dropped;
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

}
