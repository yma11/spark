/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.memory;

import com.intel.oap.common.storage.stream.DataStore;
import com.intel.oap.common.storage.stream.PMemManager;

import org.apache.spark.SparkEnv;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.package$;
import org.apache.spark.util.Utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PMemManagerInitializer {

    private static DataStore dataStore;
    private static PMemManager pMemManager;
    private static Properties properties;
    public static Properties getProperties() {
        if (properties == null) {
            synchronized (Properties.class) {
                if (properties == null) {
                    ConfigEntry<String> pMemPropertiesFile =
                            package$.MODULE$.PMEM_PROPERTY_FILE();

                    String filePath = SparkEnv.get() == null? pMemPropertiesFile.defaultValue().get(): SparkEnv.get().conf().get(pMemPropertiesFile);
                    System.out.println(filePath);
                    Properties pps = new Properties();
                    try {
                        InputStream in = Utils.getSparkClassLoader().getResourceAsStream(filePath);
                        if (in == null) {
                            in = new BufferedInputStream(new FileInputStream(filePath));
                        }
                        pps.load(in);
                        String chunkSize = pps.getProperty("chunkSize");
                        pps.setProperty("chunkSize", String.valueOf(Utils.byteStringAsBytes(chunkSize)));
                        String totalSize = pps.getProperty("totalSize");
                        pps.setProperty("totalSize", String.valueOf(Utils.byteStringAsBytes(totalSize)));
                        String initialSize = pps.getProperty("initialSize");
                        pps.setProperty("initialSize", String.valueOf(Utils.byteStringAsBytes(initialSize)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    properties = pps;
                }
            }
        }
        return properties;
    }
    public static PMemManager getPMemManager() {
        {
            if (pMemManager == null) {
                synchronized (PMemManager.class) {
                    if (pMemManager == null) {
                        pMemManager = new PMemManager(getProperties());
                    }
                }
            }
            return pMemManager;
        }
    }
    public static DataStore getDataStore() {
        if (dataStore == null) {
            synchronized (DataStore.class) {
                if (dataStore == null) {
                    dataStore = new DataStore(getPMemManager(), getProperties());
                }
            }
        }
        return dataStore;
    }
}
