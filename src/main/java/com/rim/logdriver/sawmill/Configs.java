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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public enum Configs {

  // Global configs
  hadoopConfigPaths("hadoop.config.paths",
      "file:///etc/hadoop/conf/core-site.xml file:///etc/hadoop/conf/hdfs-site.xml"),

  kerberosPrincipal("kerberos.principal", ""),

  kerberosKeytab("kerberos.keytab", ""),

  threadpoolSize("threadpool.size", Integer.toString(Runtime.getRuntime()
      .availableProcessors() * 2)),

  paths("paths", null),

  defaultTcpMaxLineLength("default.tcp.max.line.length", "4096"),

  // Per path configs
  name("name", null),

  tcpMaxLineLength("tcp.max.line.length", null),

  tcpReadBufferSize("tcp.read.buffer.size", "2048"),

  bindAddress("bind.address", "0.0.0.0"),

  port("port", null),

  charset("charset", "UTF-8"),

  outputBuckets("output.buckets", "1"),

  queueCapacity("queue.capacity", "100"),

  filePathTemplate("file.path.template", null),

  fileRotateInterval("file.rotate.interval", "600"), // seconds

  hdfsProxyUser("hdfs.proxy.user", null),

  hdfsBlockSize("hdfs.block.size", Integer.toString(256 * 1024 * 1024)),

  hdfsReplicas("hdfs.replicas", "3"),

  hdfsBufferSize("hdfs.buffer.size", "4096"),

  boomDeflateLevel("boom.deflate.level", "6"),

  boomSyncInterval("boom.sync.interval", Integer.toString(2 * 1024 * 1024));

  private final String property;
  private final String defaultValue;

  Configs(String property, String defaultValue) {
    this.property = property;
    this.defaultValue = defaultValue;
  }

  public String getProperty() {
    return property;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String get(Properties conf) {
    return getString(conf);
  }

  public String getString(Properties conf) {
    return conf.getProperty(property, defaultValue);
  }

  public Short getShort(Properties conf) {
    String value = getString(conf);
    if (value == null) {
      return null;
    }
    return Short.parseShort(value);
  }

  public Integer getInteger(Properties conf) {
    String value = getString(conf);
    if (value == null) {
      return null;
    }
    return Integer.parseInt(value);
  }

  public Long getLong(Properties conf) {
    String value = getString(conf);
    if (value == null) {
      return null;
    }
    return Long.parseLong(value);
  }

  public String[] getArray(Properties conf) {
    String value = getString(conf);
    if (value == null) {
      return null;
    }
    List<String> list = new ArrayList<String>();
    String[] rawSplit = value.split(" ");
    for (String s : rawSplit) {
      s.trim();
      if (s.isEmpty()) {
        continue;
      }
      list.add(s);
    }
    return list.toArray(new String[] {});
  }

}
