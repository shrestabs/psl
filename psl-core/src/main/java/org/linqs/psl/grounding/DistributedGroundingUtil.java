/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2019 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.linqs.psl.grounding;

import org.linqs.psl.util.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.NoSuchFieldException;
import java.util.Arrays;
import java.util.List;

/**
 * Determine the role of the node.
 */
public class DistributedGroundingUtil {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingUtil.class);
    public static final String DOMAIN_NAME = ".soe.ucsc.edu";
    public static String masterNodeName = "seacliff";
    public static List<String> slaveNodeNameList = Arrays.asList("sozopol", "sunset", "information-cast.local");
    public static boolean isMaster = false;

    private DistributedGroundingUtil() {}

    public static boolean isNodeRoleMaster() {
        String hostname = SystemUtils.getHostname();
        log.info("Hostname is " + hostname);
        System.out.println("Masternode name is " + masterNodeName);
        System.out.println(slaveNodeNameList);
        if (hostname.equals(masterNodeName)) {
            isMaster = true;
        }
        else if (slaveNodeNameList.contains(hostname)) {
            // do nothing
        }
        else {
            try {
                throw new NoSuchFieldException(String.format("Hostname %s unsupported. Role unknown.", hostname));
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        return isMaster;
    }
}