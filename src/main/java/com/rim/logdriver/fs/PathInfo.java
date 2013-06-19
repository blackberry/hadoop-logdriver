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

package com.rim.logdriver.fs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PathInfo implements Comparable<PathInfo> {
  private static final String dcNumberRegex = "\\d+";
  private static final String serviceRegex = ".+";
  private static final String dateRegex = "\\d{4}(?:0[1-9]|1[0-2])(?:[0-2]\\d|3[01])";
  private static final String hourRegex = "(?:[01][0-9]|2[0-3])";
  private static final String componentRegex = "[^/]+";

  private static final Pattern dcNumberPattern = Pattern.compile(dcNumberRegex);
  private static final Pattern servicePattern = Pattern.compile(serviceRegex);
  private static final Pattern datePattern = Pattern.compile(dateRegex);
  private static final Pattern hourPattern = Pattern.compile(hourRegex);
  private static final Pattern componentPattern = Pattern
      .compile(componentRegex);

  private final Pattern pathPattern;

  private String dcNumber;
  private String service;
  private String date;
  private String hour;
  private String component;

  private String logdir;

  public PathInfo() {
    logdir = "logs";
    pathPattern = Pattern.compile("/service/(" + dcNumberRegex + ")/("
        + serviceRegex + ")/" + logdir + "/(" + dateRegex + ")/(" + hourRegex
        + ")/(" + componentRegex + ")/?");
  }

  public PathInfo(String path) throws Exception {
    logdir = "logs";
    pathPattern = Pattern.compile("/service/(" + dcNumberRegex + ")/("
        + serviceRegex + ")/" + logdir + "/(" + dateRegex + ")/(" + hourRegex
        + ")/(" + componentRegex + ")/?");

    setFullPath(path);
  }

  public PathInfo(String logdir, String path) throws Exception {
    this.logdir = logdir;
    pathPattern = Pattern.compile("/service/(" + dcNumberRegex + ")/("
        + serviceRegex + ")/" + logdir + "/(" + dateRegex + ")/(" + hourRegex
        + ")/(" + componentRegex + ")/?");

    setFullPath(path);
  }

  public String getFullPath() {
    return "/service/" + dcNumber + "/" + service + "/" + logdir + "/" + date
        + "/" + hour + "/" + component;
  }

  public void setFullPath(String fullPath) throws Exception {
    Matcher m = pathPattern.matcher(fullPath);
    if (m.matches()) {
      setDcNumber(m.group(1));
      setService(m.group(2));
      setDate(m.group(3));
      setHour(m.group(4));
      setComponent(m.group(5));
    } else {
      throw new Exception("Invalid path: " + fullPath);
    }
  }

  public String getDcNumber() {
    return dcNumber;
  }

  public void setDcNumber(String dcNumber) throws Exception {
    if (dcNumberPattern.matcher(dcNumber).matches()) {
      this.dcNumber = dcNumber;
    } else {
      throw new Exception("Invalid site number.  Does not match "
          + dcNumberRegex);
    }
  }

  public String getService() {
    return service;
  }

  public void setService(String service) throws Exception {
    if (servicePattern.matcher(service).matches()) {
      this.service = service;
    } else {
      throw new Exception("Invalid service.  Does not match " + serviceRegex);
    }
  }

  public String getLogdir() {
    return logdir;
  }

  public void setLogdir(String logdir) {
    this.logdir = logdir;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) throws Exception {
    if (datePattern.matcher(date).matches()) {
      this.date = date;
    } else {
      throw new Exception("Invalid date.  Does not match " + dateRegex);
    }
  }

  public String getHour() {
    return hour;
  }

  public void setHour(String hour) throws Exception {
    if (hourPattern.matcher(hour).matches()) {
      this.hour = hour;
    } else {
      throw new Exception("Invalid hour.  Does not match " + hourRegex);
    }
  }

  public String getComponent() {
    return component;
  }

  public void setComponent(String component) throws Exception {
    if (componentPattern.matcher(component).matches()) {
      this.component = component;
    } else {
      throw new Exception("Invalid component.  Does not match "
          + componentRegex);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((component == null) ? 0 : component.hashCode());
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + ((dcNumber == null) ? 0 : dcNumber.hashCode());
    result = prime * result + ((hour == null) ? 0 : hour.hashCode());
    result = prime * result + ((service == null) ? 0 : service.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PathInfo other = (PathInfo) obj;
    if (component == null) {
      if (other.component != null)
        return false;
    } else if (!component.equals(other.component))
      return false;
    if (date == null) {
      if (other.date != null)
        return false;
    } else if (!date.equals(other.date))
      return false;
    if (dcNumber == null) {
      if (other.dcNumber != null)
        return false;
    } else if (!dcNumber.equals(other.dcNumber))
      return false;
    if (hour == null) {
      if (other.hour != null)
        return false;
    } else if (!hour.equals(other.hour))
      return false;
    if (service == null) {
      if (other.service != null)
        return false;
    } else if (!service.equals(other.service))
      return false;
    return true;
  }

  @Override
  public int compareTo(PathInfo o) {
    int retval = 0;

    retval = dcNumber.compareTo(o.dcNumber);
    if (retval != 0) {
      return retval;
    }

    retval = service.compareTo(o.service);
    if (retval != 0) {
      return retval;
    }

    retval = date.compareTo(o.date);
    if (retval != 0) {
      return retval;
    }

    retval = hour.compareTo(o.hour);
    if (retval != 0) {
      return retval;
    }

    retval = component.compareTo(o.component);

    return retval;
  }

}
