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

/** Create an index of all logs in /service on HDFS.
 *  <p>
 *  Usage: indexlogs [-t -n]
 *      -t      Print results to STDOUT in human-readable tree
 *      -n      Don't write index files into HDFS
 */
package com.rim.logdriver.util;

import java.util.regex.Pattern;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class IndexLogs {

  public static Pattern dataPattern = Pattern.compile("/\\d{8}/\\d{2}/[^/]*/(data|incoming|archive)");
  public static SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
  public static SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
  public static Date nullDate = new Date(0);

  public static class Component {
    public String DC;
    public String service;
    public String type;
    public String component;
    public Date startDate;
    public Date endDate;
    public Date archiveDate;
    public double totalSize;
    public double dataSize;
    public double archiveSize;
    public double incomingSize;

    public void addDataSize(double size) {
      this.dataSize += size;
      this.totalSize += size;
    }

    public void addIncomingSize(double size) {
      this.incomingSize += size;
      this.totalSize += size;
    }

    public void addArchiveSize(double size) {
      this.archiveSize += size;
      this.totalSize += size;
    }

    public Component(String DC, String service, String type, String component, Date date) {
      this.DC = DC;
      this.service = service;
      this.component = component;
      this.type = type;
      this.startDate = date;
      this.endDate = date;
      this.archiveDate = nullDate;
    }

    public String toJSONString() {
      String componentJSON = "{\"startDate\":" + this.startDate.getTime() + ","
          + "\"endDate\":" + this.endDate.getTime() + ","
          + "\"archiveDate\":" + this.archiveDate.getTime() + ","
          +  "\"totalSize\":" + this.totalSize + ","
          + "\"dataSize\":" + this.dataSize + ","
          + "\"archiveSize\":" + this.archiveSize + ","
          + "\"incomingSize\":" + this.incomingSize + "}";
      return componentJSON;
    }

  }

  private static void findComponents(Map<String, Map<String, Map<String, Map<String, Component>>>> data, FileSystem fs, Path path) throws FileNotFoundException, IOException, ParseException {
    // Grab FileStatus for each file in the path  
    FileStatus[] allFiles = fs.listStatus(path);
    // For each file, try to match a pattern that indicates we have identified a component.
    // If we find a match, add or update the component and return.
    try {
      for (int i = 0; i < allFiles.length; i++) {
        if (dataPattern.matcher(allFiles[i].getPath().toString()).find()) {
          updateComponent(data, fs, allFiles[i], path);       
          return;
        }
      }
      // If we got here no component was matched, so go one level deeper. 
      for (int i = 0; i < allFiles.length; i++) {
        if (allFiles[i].isDirectory()) {
          findComponents(data, fs, allFiles[i].getPath());
        }
      }
    }
    // It's possible that we don't have access to files in this path, or that the path is empty.
    catch (AccessControlException e) {
    }
    catch (FileNotFoundException e) {
    }
  }

  private static void updateComponent(Map<String, Map<String, Map<String, Map<String, Component>>>> data, FileSystem fs, FileStatus matchedFolder, Path path) throws IOException, ParseException {
    // Parse path by splitting it across slashes. To determine service (which might contain slashes) grab
    // everything after the DC name, but before the matched date string.
    String[] pathPieces = matchedFolder.getPath().toString().split("/");
    String[] servicePieces = path.toString().split(pathPieces[4] + "/");
    servicePieces = servicePieces[1].split("/" + pathPieces[pathPieces.length - 5]);
    String DC = pathPieces[4];
    String service = servicePieces[0];
    String component = pathPieces[pathPieces.length - 2];
    String type = pathPieces[pathPieces.length - 5];
    String status = pathPieces[pathPieces.length - 1];
    Date date = inputFormat.parse(pathPieces[pathPieces.length - 4]);

    // Check if there is a matching component, create one if not. 
    if (!componentExists(data, DC, service, type, component)) {
      data.get(DC).get(service).get(type).put(component, new Component(DC, service, type, component, date));
    }

    Component thisComponent = data.get(DC).get(service).get(type).get(component);

    // Update the start or end date if the current date is before or after, respectively. 
    if (date.before(thisComponent.startDate)) {
      thisComponent.startDate = date;
    } else if (date.after(thisComponent.endDate)) {
      thisComponent.endDate = date; 
    }

    // Is the current folder an archive? If so and date is later than the current archiveDate, update it. 
    if (status.matches("archive") && date.after(thisComponent.archiveDate)) {
      thisComponent.archiveDate = date;
    }

    // Add size data
    if (status.matches("data")) {
      thisComponent.addDataSize(fs.getContentSummary(matchedFolder.getPath()).getLength());
    } else if (status.matches("incoming")) {
      thisComponent.addIncomingSize(fs.getContentSummary(matchedFolder.getPath()).getLength());
    } else if (status.matches("archive")) {
      thisComponent.addArchiveSize(fs.getContentSummary(matchedFolder.getPath()).getLength());
    }
  }

  public static boolean componentExists(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String DC, String service, String type, String component) {
    // Determine if there is an entry for this DC. If not, create one.
    if (data.get(DC) == null) {
      data.put(DC, new HashMap<String, Map<String, Map<String, Component>>>());
    } 
    // Determine if there is an entry for this DC/service. If not, create one.
    if (data.get(DC).get(service) == null) {
      data.get(DC).put(service, new HashMap<String, Map<String, Component>>()); 
    } 
    // Determine if there is an entry for this DC/service/type. If not, create one.
    if (data.get(DC).get(service).get(type) == null) {
      data.get(DC).get(service).put(type, new HashMap<String, Component>());
    }          
    // Determine if there is an entry for this DC/service/type/component. If not, return false. 
    if (data.get(DC).get(service).get(type).get(component) == null) {
      return false;
    }
    // If we got here, there was already an entry for this component. Return true. 
    return true;
  }

  private static void humanPrint(Map<String, Map<String, Map<String, Map<String, Component>>>> data) {
    // For each DC, service, type and component, print out the collected data in a visual 'tree' structure.
    for (String DC : data.keySet()) {
      System.out.println("\n" + DC);
      for (String service : data.get(DC).keySet()) {
        System.out.println("  ᶫ " + service);
        for (String type : data.get(DC).get(service).keySet()) {
          System.out.println("      ᶫ " + type);
          for (String component : data.get(DC).get(service).get(type).keySet()) {
            Component thisComponent = data.get(DC).get(service).get(type).get(component);
            System.out.println("          ᶫ " + thisComponent.component + " (" 
                + outputFormat.format(thisComponent.startDate) + " - " 
                + outputFormat.format(thisComponent.endDate) + ", archived from " 
                + outputFormat.format(thisComponent.archiveDate) + ") (total size = " 
                + thisComponent.totalSize + " bytes, data size = " 
                + thisComponent.dataSize + " bytes, incoming size = " 
                + thisComponent.incomingSize + " bytes, archive size = " 
                + thisComponent.archiveSize + " bytes)");
          }
        }
      }
    }
  }

  private static void writeCSV(Map<String, Map<String, Map<String, Map<String, Component>>>> data, FSDataOutputStream outputFile) throws IOException {
    // Create CSV file header
    outputFile.writeBytes("DC, Service, Type, Component, Start Date, End Date, Archive Date, Total Size, Data Size, Incoming Size, Archive Size\n");

    // For each component in data, print a CSV line with all relevant information.
    for (String DC : data.keySet()) {
      for (String service : data.get(DC).keySet()) {
        for (String type : data.get(DC).get(service).keySet()) {
          for (String component : data.get(DC).get(service).get(type).keySet()) {
            Component thisComponent = data.get(DC).get(service).get(type).get(component);
            String thisLine = thisComponent.DC + ", " 
                + thisComponent.service + ", " 
                + thisComponent.type + ", " 
                + thisComponent.component + ", " 
                + thisComponent.startDate.getTime() + ", " 
                + thisComponent.endDate.getTime() + ", " 
                + thisComponent.archiveDate.getTime() + ", " 
                + thisComponent.totalSize + ", " 
                + thisComponent.dataSize + ", " 
                + thisComponent.incomingSize + ", " 
                + thisComponent.archiveSize + "\n";
            outputFile.writeBytes(thisLine);
          }
        }
      }
    }
  }

  private static void writeJSON(Map<String, Map<String, Map<String, Map<String, Component>>>> data, FSDataOutputStream outputFile) throws IOException {
    String JSONString = "{";
    Iterator<String> DCIterator = data.keySet().iterator();
    while (DCIterator.hasNext()) {
      String DC = DCIterator.next();
      JSONString += "\"" + DC + "\":{";
      Iterator<String> serviceIterator = data.get(DC).keySet().iterator();
      while (serviceIterator.hasNext()) {
        String service = serviceIterator.next();
        JSONString += "\"" + service + "\":{";
        Iterator<String> typeIterator = data.get(DC).get(service).keySet().iterator();
        while (typeIterator.hasNext()) {
          String type = typeIterator.next();
          JSONString += "\"" + type + "\":{";
          Iterator<String> componentIterator = data.get(DC).get(service).get(type).keySet().iterator();
          while (componentIterator.hasNext()) {
            String component = componentIterator.next();
            JSONString += "\"" + component + "\":" + data.get(DC).get(service).get(type).get(component).toJSONString();
            if (componentIterator.hasNext()) {
              JSONString += ",";
            }
          }
          JSONString += "}";
          if (typeIterator.hasNext()) {
            JSONString += ",";
          }
        }
        JSONString += "}";
        if (serviceIterator.hasNext()) {
          JSONString += ",";
        }
      }
      JSONString += "}";
      if (DCIterator.hasNext()) {
        JSONString += ",";
      }
    }
    JSONString += "}";
    outputFile.writeBytes(JSONString);
  }


  public static void main(String args[]) throws IOException, ParseException {
    // Create blank map for components
    Map<String, Map<String, Map<String, Map<String, Component>>>> data = new HashMap<String, Map<String, Map<String, Map<String, Component>>>>();

    // Set the output format
    Boolean humanReadable = false;
    Boolean writeIndex = true;

    for (int i = 0; i < args.length; i++) {
      if(args[i].matches("-t")) {
        humanReadable = true;
      } else if (args[i].matches("-n")) {
        writeIndex = false;
      } else {
        System.out.println("Usage: indexlogs [-t -n]\n    -t      Print results to STDOUT in human-readable tree\n" +
                                                     "    -n      Don't write index files into HDFS\n");
        System.exit(0);
      }
    }

    // Set up HDFS filesystem
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // Search the /service folder for matching paths
    if (!humanReadable && !writeIndex) {
      System.out.println("Warning: -n set without -t, not doing anything.\n");
      System.exit(0);
    } 
    System.out.println("Indexing logs...");
    findComponents(data, fs, new Path("/service"));

    // Output the generated index
    if (humanReadable) {
      humanPrint(data);
      System.out.println("");
    } 
    if (writeIndex) {
      long currentTime = System.currentTimeMillis()/1000;
      FSDataOutputStream outputCSV = fs.create(new Path("/service/_index/logindex." + currentTime + ".csv"));
      writeCSV(data, outputCSV);
      outputCSV.close();
      System.out.println("Index files written to /service/_index/logindex." + currentTime + ".csv and /service/_index/logindex." + currentTime + ".json");
      FSDataOutputStream outputJSON = fs.create(new Path("/service/_index/logindex." + currentTime + ".json"));
      writeJSON(data, outputJSON);
      outputJSON.close();
    }
  }
}
