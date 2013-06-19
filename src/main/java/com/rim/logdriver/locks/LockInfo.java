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

package com.rim.logdriver.locks;

public class LockInfo implements Comparable<LockInfo> {
  private String path;
  private long lastModified;
  private int readLockCount;
  private int writeLockCount;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getLastModified() {
    return lastModified;
  }

  public void setLastModified(long lastModified) {
    this.lastModified = lastModified;
  }

  public int getReadLockCount() {
    return readLockCount;
  }

  public void setReadLockCount(int readLockCount) {
    this.readLockCount = readLockCount;
  }

  public int getWriteLockCount() {
    return writeLockCount;
  }

  public void setWriteLockCount(int writeLockCount) {
    this.writeLockCount = writeLockCount;
  }

  @Override
  public String toString() {
    return path + "\tLastModified:" + lastModified + "\tReadLockCount:"
        + readLockCount + "\tWriteLockCount:" + writeLockCount;
  }

  @Override
  public int compareTo(LockInfo o) {
    return path.compareTo(o.path);
  }

}
