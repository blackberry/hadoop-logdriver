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

package com.rim.logdriver;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static class used to get access to schemas used by the HLog system.
 * 
 * @author Will Chartrand <wchartrand@rim.com>
 * 
 */
public class Schemas {
  private static final Logger LOG = LoggerFactory.getLogger(Schemas.class);

  private static boolean initialized = false;
  private static Map<String, Schema> schemaMap = new HashMap<String, Schema>();

  /**
   * Private constuctor.
   */
  private Schemas() {
  }

  /**
   * Gets a schema from the known list. Probably by loading it from a file.
   * 
   * @param name
   *          The name of the schema to get.
   * @return the Schema that was loaded, or null if there was a problem getting
   *         it.
   */
  public static Schema getSchema(String name) {
    // First, load in known, hardcoded, schemas.
    if (!initialized) {
      init();
    }

    if (schemaMap.containsKey(name)) {
      return schemaMap.get(name);
    }

    // First, try and load the file <name>.schema. If it exists, it should
    // be in the resources directory, and therefore in the classpath.
    String fileName = name + ".schema";
    InputStream inputStream = ClassLoader.getSystemResourceAsStream(fileName);
    if (inputStream == null) {
      LOG.info("Failed to load schema for name '{}'.  File did not exist.",
          name);
      return null;
    }

    // Since ther file exists, try to parse it. In case of failure, just log
    // it and return null.
    Schema.Parser parser = new Schema.Parser();
    Schema schema = null;
    try {
      schema = parser.parse(inputStream);
    } catch (IOException e) {
      LOG.warn("Error parsing schema from '{}'.", fileName, e);
      return null;
    }

    return schema;
  }

  private static synchronized void init() {
    if (initialized) {
      return;
    }

    // Load schemas into map.
    Schema.Parser parser = new Schema.Parser();

    parser = new Schema.Parser();
    Schema logBlock = parser
        .parse("{ " + //
            "\"type\": \"record\", " + //
            "\"name\": \"logBlock\", " + //
            "\"fields\": [ " + //
            "{ \"name\": \"second\", \"type\": \"long\" }, " + //
            "{ \"name\": \"createTime\", \"type\": \"long\" }, " + //
            "{ \"name\": \"blockNumber\", \"type\": \"long\" }, " + //
            "{\"name\": \"logLines\", \"type\": { " + //
            "\"type\": \"array\", " + //
            "\"items\": { \"type\": \"record\", \"name\": \"messageWithMillis\", \"fields\": [ {\"name\": \"ms\", \"type\": \"long\" }, {\"name\": \"eventId\", \"type\": \"int\", \"default\": 0 }, {\"name\": \"message\", \"type\": \"string\" } ] } }} ] }");
    schemaMap.put("logBlock", logBlock);

    initialized = true;
  }
}
