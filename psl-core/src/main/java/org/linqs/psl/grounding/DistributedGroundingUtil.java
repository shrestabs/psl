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

//TODO: remove unnecessary imports
import org.linqs.psl.config.Config;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.QueryResultIterable;
import org.linqs.psl.database.atom.AtomManager;
import org.linqs.psl.database.rdbms.QueryRewriter;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.QueryAtom;
import org.linqs.psl.model.Model;
import org.linqs.psl.model.formula.Formula;
import org.linqs.psl.model.formula.Conjunction;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.model.term.Variable;
import org.linqs.psl.model.term.Term;
import org.linqs.psl.util.Parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Integer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.Variable;

import org.linqs.psl.util.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.NoSuchFieldException;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

/**
 * Determine the role of the node.
 */
public class DistributedGroundingUtil {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingUtil.class);
    public static final String DOMAIN_NAME = ".soe.ucsc.edu";
    //public static String masterNodeName = "seacliff";
    public static String masterNodeName = "eduroam-169-233-205-33.ucsc.edu";
    final static int port = 6066;
    public static List<String> slaveNodeNameList = Arrays.asList("sozopol", "sunset", "seacliff");
    public static boolean isMaster = false;

    private DistributedGroundingUtil() {}

    public static Constant stringToConstant(String constant_string) {
        Constant newConstant = ConstantType.getConstant(constant_string, ConstantType.UniqueStringID);
        return newConstant;
    }

    public static Map<Variable, Integer> stringMapToVariableMap(Map<String, Integer> outMap) {
        Map<Variable, Integer> outVariableMap = new HashMap<Variable, Integer>();
        for(Map.Entry<String, Integer> varMap : outMap.entrySet()) {
            String key = varMap.getKey();
            int value = varMap.getValue();
            Variable varTerm = new Variable(key);
            outVariableMap.put(varTerm, value);
        }
        return outVariableMap;
    }

    public static Constant[] stringArrayToConstant(String [] stringConstant) {
        int stringArrayLength = stringConstant.length;
        Constant[] constArray = new Constant[stringArrayLength];
        for(int i = 0; i < stringArrayLength; i++) {
            String constantString = stringConstant[i];
            constArray[i] = stringToConstant(constantString);
        }
        return constArray;
    }

    public static ByteBuffer stringToByteBuffer (String stringBuffer) throws UnsupportedEncodingException {
        return ByteBuffer.wrap(stringBuffer.getBytes("UTF-8"));
    }

    public static String ByteBufferToString (ByteBuffer byteBuffer) throws UnsupportedEncodingException {
        return new String(byteBuffer.array(), "UTF-8");
    }

    public static boolean isNodeRoleMaster() {
        String hostname = SystemUtils.getHostname();
        log.info("Hostname is " + hostname);
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
    /*
     *If the two maps aren't the same, reorder the array based on varMap1.
    */
    public static List<String []> reorderArray(Map<Variable, Integer> varMap1, Map<Variable, Integer> varMap2, List<String[]>constList) {
        if(varMap1.equals(varMap2)) {
            return constList;
        } 
        List<String[]> newConstantList = new ArrayList<String[]>();
        for(String[] constantStringArray : constList) {
            int rowLength = constantStringArray.length;
            String[] row = new String[rowLength];

            for(Map.Entry<Variable, Integer> varMap : varMap1.entrySet()) {
                Variable key = varMap.getKey();
                int value = varMap.getValue();

                row[value] = constantStringArray[varMap2.get(key)];
            }
            newConstantList.add(row);

        } 
        return newConstantList;      

    }

}
