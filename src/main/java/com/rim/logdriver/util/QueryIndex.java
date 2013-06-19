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

/** Return data for services contained in the latest index in /service/_index
 *  <p>
 *  Queryindex will display disk use and available dates for logged data.
        Usage: queryindex [options] ['DC'] ['service'] ['type'] ['component']
        Each of DC, service, type and component can be a regex
          -p                          Print names of individual matched components
          -t 'start' 'end'            Display only components with indexed data between these dates
        The following options require -p
          -d                          Print the available date range for each component
          -s                          Print the total size of each component
          -i                          Print average daily ingest for the lifetime of this component
          -l                          Print all data for each component on a single line
          -a                          Display ingest activity between the specified dates, requires -t*
        * Display of ingest activity requires multiple queries to HDFS (can be slow)
 */

package com.rim.logdriver.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.rim.logdriver.util.IndexLogs.Component;

public class QueryIndex {

  public static SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
  public static SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
  public static Date nullDate = new Date(0);
  
  public static final long oneHour = 3600000;
  public static final long oneDay = 24 * oneHour;
    
  private Path getLatestIndex(FileSystem fs) throws FileNotFoundException, IOException {
    // Get path of latest index file
    FileStatus[] indexFiles = fs.listStatus(new Path("/service/_index/"));
    long latestDate = 0;
    for (int i = 0; i < indexFiles.length; i++) {
      try {
        if (indexFiles[i].getPath().toString().matches(".*\\.json$")) {
          String[] pathPieces = indexFiles[i].getPath().toString().split("\\.");
          long currentDate = Integer.parseInt(pathPieces[pathPieces.length - 2]);
          if (currentDate > latestDate) {
            latestDate = currentDate;
          }
        }
      }
      catch (NumberFormatException e) {} catch (IndexOutOfBoundsException e) {}
    }
    if (latestDate > 0) {
      Date now = new Date();
      Long age = (now.getTime() - (latestDate * 1000)) / oneHour;
      if (age > 24) {
        System.out.println("\nWarning: Index is over 24 hours old!");
      }
      System.out.println("\nIndex is from " + outputFormat.format(latestDate * 1000) + " and is " + age + " hours old.\n");
      return new Path("/service/_index/logindex." + latestDate + ".json");
    } 
    else {
      return null;
    }
  }

  private void readJSONIntoMap(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String JSONString) throws JSONException {
    JSONObject indexJSON = new JSONObject(JSONString);
    Iterator<String> DCIterator = indexJSON.keys();
    while(DCIterator.hasNext()) {
      String thisDC = DCIterator.next();
      JSONObject serviceJSON = new JSONObject(indexJSON.getString(thisDC));
      Iterator<String> serviceIterator = serviceJSON.keys();
      while(serviceIterator.hasNext()) {
        String thisService = serviceIterator.next();
        JSONObject typeJSON = new JSONObject(serviceJSON.getString(thisService));
        Iterator<String> typeIterator = typeJSON.keys();
        while(typeIterator.hasNext()) {
          String thisType = typeIterator.next();
          JSONObject componentJSON = new JSONObject(typeJSON.getString(thisType));
          Iterator<String> componentIterator = componentJSON.keys();
          while(componentIterator.hasNext()) {
            String thisComponent = componentIterator.next();
            JSONObject componentInfo = new JSONObject(componentJSON.getString(thisComponent));

            // Read in all information for this component
            Date startDate = new Date(componentInfo.getLong("startDate"));
            Date endDate = new Date(componentInfo.getLong("endDate"));
            Date archiveDate;
            if(componentInfo.getLong("archiveDate") > 0) {
              archiveDate = new Date(componentInfo.getLong("archiveDate"));              
            } else {
              archiveDate = nullDate;
            }
            double totalSize = componentInfo.getDouble("totalSize");
            double dataSize = componentInfo.getDouble("dataSize");
            double archiveSize = componentInfo.getDouble("archiveSize");
            double incomingSize = componentInfo.getDouble("incomingSize");

            // The component shouldn't already exist if this is a valid index, but check anyway since
            // checking will create a blank component entry in the nested map. 
            if(!IndexLogs.componentExists(data, thisDC, thisService, thisType, thisComponent)) {
              data.get(thisDC).get(thisService).get(thisType).put(thisComponent, new Component(thisDC, thisService, thisType, thisComponent, startDate));
            }

            // Load data into new component
            Component component = data.get(thisDC).get(thisService).get(thisType).get(thisComponent);
            component.endDate = endDate;
            component.archiveDate = archiveDate;
            component.totalSize = totalSize;
            component.dataSize = dataSize;
            component.archiveSize = archiveSize;
            component.incomingSize = incomingSize;

          }
        }
      }
    }
  }
  
  private Set<String> listDCs(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String searchString) {
    Set<String> returnDCs = new HashSet<String>();
    Pattern DCPattern = Pattern.compile(searchString);
    
    for (String DC : data.keySet()) {
      if (DCPattern.matcher(DC).find()) {
        returnDCs.add(DC);
      }
    }
    return returnDCs;
  }
  
  private Set<String> listServices(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String DC, String searchString) {
    Set<String> returnServices = new HashSet<String>();
    Pattern DCPattern = Pattern.compile(searchString);

    for (String service : data.get(DC).keySet()) {
      if (DCPattern.matcher(service).find()) {
        returnServices.add(service);
      }
    } 
    return returnServices;
  }

  private Set<String> listTypes(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String DC, String service, String searchString) {
    Set<String> returnTypes = new HashSet<String>();
    Pattern typePattern = Pattern.compile(searchString);
    
    for (String type : data.get(DC).get(service).keySet()) {
      if (typePattern.matcher(type).find()) {
        returnTypes.add(type);
      }
    }
    return returnTypes;
  }
  
  private Set<String> listComponents(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String DC, String service, String type, String searchString) {
    Set<String> returnComponents = new HashSet<String>();
    Pattern componentPattern = Pattern.compile(searchString);
    
    for (String component : data.get(DC).get(service).get(type).keySet()) {
      if (componentPattern.matcher(component).find()) {
        returnComponents.add(component);
      }
    }
    return returnComponents;
  }
  
  private Set<Component> matchedComponents(Map<String, Map<String, Map<String, Map<String, Component>>>> data, String DCString, String serviceString, String typeString, String componentString, Date startDate, Date endDate) {
    Set<Component> matchingComponents = new HashSet<Component>();
    for (String DC : listDCs(data, DCString)) {
      for (String service : listServices(data, DC, serviceString)) {
        for (String type : listTypes(data, DC, service, typeString)) {
          for (String component : listComponents(data, DC, service, type, componentString)) {
            Component matchedComponent = data.get(DC).get(service).get(type).get(component); 
            if(matchedComponent.endDate.after(startDate) && matchedComponent.startDate.before(endDate)) {
              matchingComponents.add(matchedComponent);  
            }
          }
        }
      }
    }
    return matchingComponents;
  }
  
  private double getIngestRate(Component component) {
    double ingestTime;
    if(component.archiveDate != nullDate) {
      ingestTime = component.endDate.getTime() - component.archiveDate.getTime();
    } else {
      ingestTime = component.endDate.getTime() - component.startDate.getTime();
    }
    if (ingestTime > 0) {
      return (component.dataSize / ((ingestTime + oneDay) / oneDay));
    } else if (component.dataSize > 0) {
      return (component.dataSize);
    } else {
      return 0;
    }
  }
  
  private Date getEarliestDate(Set<Component> components) {
    Date earliestDate = new Date();
    for (Component component : components) {
      if (component.startDate.before(earliestDate)) {
         earliestDate = component.startDate;
      }
    }
    return earliestDate;
  }
  
  private Date getLatestDate(Set<Component> components) {
    Date latestDate = new Date(0);
    for (Component component : components) {
      if (component.endDate.after(latestDate)) {
         latestDate = component.endDate;
      }
    }
    return latestDate;
  }
  
  private double totalSize(Set<Component> components) {
    double totalSize = new Double(0);
    for (Component component : components) {
      totalSize += component.totalSize;
    }
    return totalSize;
  }
  
  public static String formatByteSize(double byteSize) {
    
    double K = 1024;
    double M = K * 1024;
    double G = M * 1024;
    double T = G * 1024;
    
    if (byteSize < K) {
      
      return String.format("%d", (int) byteSize) + " B";
    } else if (byteSize < M) {
      return String.format("%.2f", byteSize / K) + " KB";
    } else if (byteSize < G) {
      return String.format("%.2f", byteSize / M) + " MB";
    } else if (byteSize < T) {
      return String.format("%.2f", byteSize / G) + " GB";
    } else { 
      return String.format("%.2f", byteSize / T) + " TB";
    }
  }
  
  private void printComponents(FileSystem fs, Set<Component> components, boolean showDates, boolean showSize, boolean showIngest, boolean greppable, Date startDate, Date endDate, boolean printIngestOverTime) throws IOException, ParseException {
    String dataSeparator = "\n    ";
    if (greppable) {
      dataSeparator = " *** ";
    }
    
    double totalIngestOverTimePeriod = 0;
    double minIngestOverTimePeriod = 0;
    double maxIngestOverTimePeriod = 0;
    
    for(Component component : components) {
      System.out.print(component.DC + " / " + component.service + " / " + component.type + " / " + component.component);
      if (showDates) {
        long numDays = ((component.endDate.getTime() - component.startDate.getTime()) / oneDay) + 1;
        System.out.print(dataSeparator + "Logs exist for " + numDays + " days, from " + outputFormat.format(component.startDate) + " to " + outputFormat.format(component.endDate));
        if (component.archiveDate != nullDate) {
          System.out.print(", archived up to " + outputFormat.format(component.archiveDate));
        } else {
          System.out.print(", none archived");
        }
      }
      if (showSize) {
       System.out.print(dataSeparator + "Total: " + formatByteSize(component.totalSize) + ", Data: " + formatByteSize(component.dataSize) +  ", Archived: " + formatByteSize(component.archiveSize) + ", Incoming: " + formatByteSize(component.incomingSize));
      }
      if (showIngest) {
        System.out.print(dataSeparator + "Average daily ingest over logged period: " + formatByteSize(getIngestRate(component)) + "/day");
      }
      if (printIngestOverTime) {
        System.out.println("\n\nGathering statistics...");
        double[] plotData = LogStats.getDataOverTime(fs, component, startDate, endDate);
        LogStats.printStats(fs, component, plotData, startDate, endDate);
        totalIngestOverTimePeriod += LogStats.getDataTotal(plotData);
        if (minIngestOverTimePeriod > LogStats.getDataMin(plotData) || minIngestOverTimePeriod == 0) {
          minIngestOverTimePeriod = LogStats.getDataMin(plotData);
        }
        if (maxIngestOverTimePeriod < LogStats.getDataMax(plotData)) {
          maxIngestOverTimePeriod = LogStats.getDataMax(plotData);
        }
      }
      System.out.println("\n"); 
    }
    if (printIngestOverTime) {
      long totalHours = ((endDate.getTime() - startDate.getTime()) / oneHour); 
      System.out.println("Ingest for all matched components from " + outputFormat.format(startDate) + " to " + outputFormat.format(endDate) + ", " + totalHours + " hours total:\n" +
                       "\n  Total Ingest:        " + formatByteSize(totalIngestOverTimePeriod) +
                       "\n  Average Ingest Rate: " + formatByteSize(totalIngestOverTimePeriod /totalHours) + "/hour" +
                       "\n  Max Ingest Rate:     " + formatByteSize(maxIngestOverTimePeriod) + "/hour" +
                       "\n  Min Ingest Rate:     " + formatByteSize(minIngestOverTimePeriod) + "/hour");
    }
  }
  
  public void main(String[] args) throws IOException, JSONException, ParseException {
    
    Map<String, Map<String, Map<String, Map<String, Component>>>> data = new HashMap<String, Map<String, Map<String, Map<String, Component>>>>();
    boolean printComponents = false;
    boolean printDates = false;
    boolean printSizes = false;
    boolean printIngest = false;
    boolean greppable = false;
    boolean datesSet = false;
    Date startDate = new Date(0);
    Date endDate = new Date();
    boolean printIngestOverTime = false;
    
    String[] pathArgs = new String[] {".*", ".*", ".*", ".*"};
    int pathLevel = 0;
    
    inputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    outputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    
    // Set up HDFS filesystem
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // Get path of latest index
    Path indexPath = getLatestIndex(fs);
    if (indexPath != null) {
      BufferedReader indexFile = new BufferedReader(new InputStreamReader(fs.open(indexPath)));
      String thisLine = indexFile.readLine();
      readJSONIntoMap(data, thisLine);
    } else {
      System.out.println("No valid index files found.");
      System.exit(0);
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].matches("^-.*")) {
        // This is a command line switch
        if (args[i].matches("-p")) {
          printComponents = true;
        } else if (args[i].matches("-d")) {
          printDates = true;
        } else if (args[i].matches("-s")) {
          printSizes = true;
        } else if (args[i].matches("-i")) {
          printIngest = true;
        } else if (args[i].matches("-l")) {
          greppable = true;
        } else if (args[i].matches("-t")) {
          try {
            startDate = LogStats.roundDownToHour(new Date((Long.parseLong(args[i + 1]) * 1000)));
            endDate = LogStats.roundUpToHour(new Date((Long.parseLong(args[i + 2]) * 1000)));
            datesSet = true;
          } catch (ParseException e) {
            System.out.println("Can't parse start and end dates.");
            System.exit(0);
          }
          if (endDate.before(startDate)) {
            System.out.println("Can't plot over a negative time range.");
            System.exit(0);
          }
          i += 2;
        } else if (args[i].matches("-a")) {
          printIngestOverTime = true;
        } else {
          System.out.println("Invalid switch " + args[i]);
          System.exit(0);
        }
      } else {
        try {
          pathArgs[pathLevel] = args[i].substring(1, args[i].length()-1);
          if (pathArgs[pathLevel].matches("[*]")) {
            pathArgs[pathLevel] = ".*";
          }
          pathLevel++;
        } catch (ArrayIndexOutOfBoundsException e) {
          System.out.println("Error: Too many path arguments.");
          System.exit(0);
        }
      }
    }
     
    if (printIngestOverTime && (!datesSet || !printComponents)) {
      System.out.println("-stats set without -p or -t, aborting.");
      System.exit(0);
    }
    
    Set<Component> components = matchedComponents(data, pathArgs[0], pathArgs[1], pathArgs[2], pathArgs[3], startDate, endDate);
    if (components.size() > 0) {
      if (printComponents) {
        printComponents(fs, components, printDates, printSizes, printIngest, greppable, startDate, endDate, printIngestOverTime);
      }
      double totalIngestDays = ((getLatestDate(components).getTime() - getEarliestDate(components).getTime()) / oneDay) + 1;
      double totalIngestRate = totalSize(components) / totalIngestDays;
      System.out.println("\nTotals for all matched components:\n\n" +
    	  	         "  Total Size:    " + formatByteSize(totalSize(components)) + "\n" +
    		         "  Ingest Rate:   " + formatByteSize(totalIngestRate) + "/day\n" +
    		         "  Earliest Date: " + outputFormat.format(getEarliestDate(components)) + "\n" + 
    		         "  Latest Date:   " + outputFormat.format(getLatestDate(components)) + "\n" +
    		         "  Total Time:    " + (int) totalIngestDays + " days\n");
    } else {
      System.out.println("\nNo components found.\n");
    }
  }
}
