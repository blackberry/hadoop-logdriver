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
 * Package of tools used to collect and display log ingest for services
 * within a specified time period. Called by com.rim.logdriver.util.quertyindex
 * 
 */

package com.rim.logdriver.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rim.logdriver.util.IndexLogs.Component;

public class LogStats {

  public static SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
  public static SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
  public static SimpleDateFormat outputFormatHours = new SimpleDateFormat("yyyy-MM-dd HH");
  public static SimpleDateFormat outputTimeOnly = new SimpleDateFormat("HH:mm");
  public static Date nullDate = new Date(0);
  public static final long oneHour = 3600000;
  public static final long oneDay = 24 * oneHour;
  
  public static double getDataMax(double[] logVolumes) {
    double max = 0;
    for (int i = 0; i < logVolumes.length; i++) {
      if (logVolumes[i] > max) {
        max = logVolumes[i];
      }
    } 
    return max;
  }
  
  public static double getDataMin(double[] logVolumes) {
    double min = getDataMax(logVolumes);
    for (int i = 0; i < logVolumes.length; i++) {
      if (logVolumes[i] < min) {
        min = logVolumes[i];
      }
    } 
    return min;
  }
  
  public static double getDataTotal(double[] logVolumes) {
    double total = 0;
    for (int i = 0; i < logVolumes.length; i++) {
      total += logVolumes[i];
    }
    return total;
  }

  // Use the existing date format to round up or down to the nearest out
  
  public static Date roundDownToHour(Date date) throws ParseException {
    return outputFormatHours.parse(outputFormatHours.format(date));
  }
  
  public static Date roundUpToHour(Date date) throws ParseException {
    return outputFormatHours.parse(outputFormatHours.format(new Date(date.getTime() + oneHour - 1)));
  }
  
  public static double[] getDataOverTime(FileSystem fs, Component component, Date startDate, Date endDate) {
    
    if (startDate.after(component.endDate) || endDate.before(component.startDate)) {
      return new double[0];
    }
    
    // If the date range specified overlaps archived data, notify the user 
    if (startDate.before(component.archiveDate)) {
      System.out.println("Warning: Time range specified includes archived data");
    }
    
    // Set up variable and array. Fill with -1 to indicate if hours are unused at the end. 
    long totalHours = (endDate.getTime() - startDate.getTime()) / oneHour;   
    int logVolumesIndex = 0;
    double[] logVolumes = new double[(int) totalHours];
    String basePath = "/service/" + component.DC + "/" + component.service + "/" + component.type + "/";

    for (Long currentDate = startDate.getTime(); currentDate < endDate.getTime(); currentDate += oneHour) {
      String dateAndHour = inputFormat.format(new Date(currentDate)) + "/" + String.format("%02d", new Date(currentDate).getHours()) + "/";
      Path path = new Path(basePath + dateAndHour + component.component);
      if (component.startDate.getTime() - oneDay < currentDate && component.endDate.getTime() + oneDay > currentDate) {
        try {
          logVolumes[logVolumesIndex] = fs.getContentSummary(path).getLength();
        } catch (IOException e) {
          logVolumes[logVolumesIndex] = 0;
        }
      } else {
        logVolumes[logVolumesIndex] = 0;
      }
      logVolumesIndex++;
    }
    return logVolumes;
  }

  public static void printStats(FileSystem fs, Component component, double[] logVolumes, Date startDate, Date endDate) throws ParseException {
    int totalHours = logVolumes.length;
 
    if(logVolumes.length == 0) {    
      System.out.println("\n    No indexed data between " + outputFormatHours.format(startDate) + "h and " + outputFormatHours.format(endDate) + "h.");
      return;
    }
    
    // Calculate average ingest over specified period
    double totalIngest = 0;
    for (int i = 0; i < totalHours; i++) {
      totalIngest += logVolumes[i];
    }
    double averageIngest = totalIngest / totalHours;
    
    int height = 11;
    int width = 61;
    if (totalHours < width) {
      width = totalHours;
    }
    double columnHeights[] = new double[width];
    Arrays.fill(columnHeights, 0);
    double hoursPerColumn = (double) totalHours / width;

    // Calculate column heights
    int hour = 0;
    double hoursLeft = 0;
    for (int column = 0; column < width; column++) {
      columnHeights[column] += (1 - hoursLeft) * logVolumes[hour] / hoursPerColumn;
      hour++;
      hoursLeft = hoursPerColumn - (1 - hoursLeft);
      while (hoursLeft >= 1 && hour < totalHours) {
        columnHeights[column] += logVolumes[hour] / hoursPerColumn;
        hour++;
        hoursLeft--;
      }
      if (hour < totalHours) {
        columnHeights[column] += hoursLeft * logVolumes[hour] / hoursPerColumn;
      } else {
        break;
      }
    }        

    double max = getDataMax(columnHeights);
    double min = getDataMin(columnHeights);
    if (max <= min) {
      max = min + 1;
    }
    double range = max - min;
    
    System.out.println("\n    Activity from " + outputFormatHours.format(startDate) + "h to " + outputFormatHours.format(endDate) + "h inclusive, " + totalHours + " hours total.");
    System.out.println("    Ingest over this period was a total of " + QueryIndex.formatByteSize(getDataTotal(logVolumes)) + " at an average of " + QueryIndex.formatByteSize(averageIngest) + "/hour.");
    System.out.println("    Peak ingest over this period was " + QueryIndex.formatByteSize(getDataMax(logVolumes)) + "/hour and minimum ingest was " + QueryIndex.formatByteSize(getDataMin(logVolumes)) + "/hour.");
    System.out.print("\n" + String.format("%9s",QueryIndex.formatByteSize(max)) + "/hour - ");
    
    // Display the plot
    for (double level = height; level > 0; level--) {
      if (level == (height / 2) + 1) {
        System.out.print("    Ingest       ");
      } else if (level != height) {
        System.out.print("                 ");
      } 
      for (int column = 0; column < width; column++) {
        if (columnHeights[column] - min >= ((level - 0.33) * range) / height) {
          System.out.print("█");
        } else if (columnHeights[column] - min > ((level - 0.66) * range) / height) {
          System.out.print("▄");
        } else {
          System.out.print(" ");
        }
      }
      System.out.println("");
    }
    
    // Create bottom axis of plot, a solid line with outside ticks
    System.out.print(String.format("%9s",QueryIndex.formatByteSize(min)) + "/hour - ");
    double timePosition = startDate.getTime();
    for (int column = 0; column < width; column++) {
      if (column % 12 == 0) {
        System.out.print("█");
      } else {
        System.out.print("▀");
      }
    }
    
    // Display the hour for each tick
    System.out.print("\n               ");
    timePosition = startDate.getTime();
    for (int column = 0; column < width; column++) {
      if (column % 12 == 0) {
        Date timeToPrint = roundDownToHour(new Date((long) (timePosition + (hoursPerColumn/2))));
        System.out.print(outputTimeOnly.format(timeToPrint) + "       ");
      }
      timePosition += oneHour * hoursPerColumn;
    }
   
    // Display the date for each tick
    System.out.print("\n             ");
    timePosition = startDate.getTime();
    for (int column = 0; column < width; column++) {
      if (column % 12 == 0) {
        System.out.print(outputFormat.format(new Date((long) timePosition)) + "  ");
      }
      timePosition += oneHour * hoursPerColumn;
    }
    
    // Print x-axis label
    System.out.println("\n                               Time (GMT), " + String.format("%.02f", hoursPerColumn) + " hours per column");
  }
} 
