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

package com.rim.logdriver.mapreduce.boom;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.rim.logdriver.boom.LogLineData;

public class BoomFilterMapper extends
    Mapper<LogLineData, Text, LogLineData, Text> {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoomFilterMapper.class);
  private List<Filter> filters = null;

  @SuppressWarnings("unchecked")
  public static List<Filter> loadFilters(String componentName,
      InputStream filterFile) {
    List<Filter> filters = new ArrayList<Filter>();

    Yaml yaml = new Yaml();
    Map<Object, Object> filterConf;
    filterConf = (Map<Object, Object>) yaml.load(filterFile);

    // Check each conf set, to see which one we should use (if any)
    FILTERS: for (Map<Object, Object> confSet : (List<Map<Object, Object>>) filterConf
        .get("filters")) {
      for (Entry<Object, Object> e : confSet.entrySet()) {
        String key = e.getKey().toString();

        if (componentName.matches(key)) {
          for (Map<Object, Object> confItem : (List<Map<Object, Object>>) confSet
              .get(key)) {
            String type = (String) confItem.get("type");
            String pattern = (String) confItem.get("pattern");

            if (type == null) {
              LOG.warn("Can't get process configuration item [no type] {}",
                  confItem);
              continue;
            }

            if ("regex".equals(type.toLowerCase())) {
              if (pattern == null) {
                LOG.warn(
                    "Can't get process configuration item [no pattern] {}",
                    confItem);
                continue;
              }
              filters.add(new RegexFilter(pattern));
            } else if ("stringmatch".equals(type.toLowerCase())) {
              if (pattern == null) {
                LOG.warn(
                    "Can't get process configuration item [no pattern] {}",
                    confItem);
                continue;
              }
              filters.add(new StringMatchFilter(pattern));
            } else if ("keepall".equals(type.toLowerCase())) {
              filters.add(new KeepAllFilter());
            } else if ("dropall".equals(type.toLowerCase())) {
              filters.add(new DropAllFilter());
            } else {
              LOG.warn(
                  "Unsupported filter type '{}'.  Try 'regex' or 'stringmatch'.",
                  type);
            }
          }

          break FILTERS;
        }
      }
    }
    return filters;
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();

    // We'll need to know the component name to know which rules to use
    String componentName = conf.get("logdriver.component.name");

    // Load in the yaml file that defines the rules.
    String confFileName = conf.get("logdriver.filter.file");

    try {
      filters = loadFilters(componentName, new FileInputStream(confFileName));
    } catch (FileNotFoundException e) {
      LOG.error("Error loading config files.  No filters will be used.", e);
    }
    LOG.info("Initial filter set: {}", filters);
  }

  @Override
  protected void map(LogLineData key, Text value, Context context)
      throws IOException, InterruptedException {
    if (filters.size() == 0) {
      throw new IOException("No filters found.");
    }

    Filter thisFilter = null;
    for (int i = 0; i < filters.size(); i++) {
      thisFilter = filters.get(i);

      // Check for a match
      if (thisFilter.accept(value.toString())) {
        context.write(key, value);

        // Reorder the filters, if necessary.
        Filter tmpFilter;
        while (i > 0
            && thisFilter.getNumMatches() > filters.get(i - 1).getNumMatches()) {
          // move this filter up..
          tmpFilter = filters.get(i - 1);
          filters.set(i - 1, thisFilter);
          filters.set(i, tmpFilter);
          i--;

          LOG.info("Filter set reordered.  Currently: {}", filters);
        }

        // Stop processing filters.
        break;
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    LOG.info("Final filter status: {}", filters);
  }

  public static interface Filter {
    public boolean accept(String line);

    public int getNumMatches();
  }

  public static class StringMatchFilter implements Filter {
    private String filter;
    private int numMatches = 0;

    public StringMatchFilter(String filter) {
      this.filter = filter;
    }

    @Override
    public boolean accept(String line) {
      boolean matches = line.contains(filter);
      if (matches) {
        numMatches++;
      }
      return matches;
    }

    @Override
    public int getNumMatches() {
      return numMatches;
    }

    @Override
    public String toString() {
      return "StringMatchFilter [filter=" + filter + ", numMatches="
          + numMatches + "]";
    }
  }

  public static class RegexFilter implements Filter {
    private Pattern pattern;
    private int numMatches = 0;

    public RegexFilter(String regex) {
      pattern = Pattern.compile(regex);
    }

    @Override
    public boolean accept(String line) {
      boolean matches = pattern.matcher(line).find();
      if (matches) {
        numMatches++;
      }
      return matches;
    }

    @Override
    public int getNumMatches() {
      return numMatches;
    }

    @Override
    public String toString() {
      return "RegexFilter [pattern=" + pattern + ", numMatches=" + numMatches
          + "]";
    }
  }

  public static class KeepAllFilter implements Filter {
    private int numMatches = 0;

    public KeepAllFilter() {
    }

    @Override
    public boolean accept(String line) {
      numMatches++;
      return true;
    }

    @Override
    public int getNumMatches() {
      return numMatches;
    }

    @Override
    public String toString() {
      return "KeepAllFilter [numMatches=" + numMatches + "]";
    }
  }

  public static class DropAllFilter implements Filter {
    private int numMatches = 0;

    public DropAllFilter() {
    }

    @Override
    public boolean accept(String line) {
      return false;
    }

    @Override
    public int getNumMatches() {
      return numMatches;
    }

    @Override
    public String toString() {
      return "DropAllFilter [numMatches=" + numMatches + "]";
    }
  }

}
