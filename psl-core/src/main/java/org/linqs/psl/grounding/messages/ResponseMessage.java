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
package org.linqs.psl.grounding.messages;

import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseMessage extends Message {
    String messagename;
    private static final Logger log = LoggerFactory.getLogger(ResponseMessage.class);
    public List <String[]> outQueryResult; // List <Constant[]> outQueryResult;
    public Map<String, Integer> outVariableMap; //Map<Variable, Integer> outVariableMap;

    public ResponseMessage() {
        String messagename = "Response Message";
        outQueryResult = new ArrayList<String[]>();
        outVariableMap = new HashMap<String, Integer>();
     }

     public String serialize() {
        String buffer= "";
        message_type = (MessageType.RESPONSE).getValue();
        log.info("Added msg type first" + message_type);
        for (String[] arr : outQueryResult) {
            for (String s: arr) {
                System.out.println("Added " + s);
                buffer = buffer.concat(s + "\t");
               }
               buffer = buffer.concat("\t"); // double tab for new line
            }
        buffer = buffer.concat("\t"); // triple tab for new Data Struct

        for (Map.Entry<String, Integer> entry : outVariableMap.entrySet()) {
            System.out.println("Added " + entry.getKey() + "\t" + entry.getValue() + "\t");
            buffer = buffer.concat(entry.getKey() + "\t" + entry.getValue() + "\t");
        }

        message_size = buffer.length();
        log.info("Added msg size " + String.format("%08d", message_size));
        buffer = Integer.toString(message_type) + String.format("%08d", message_size) + buffer;
        log.info("Serialized response message to {}", buffer);
        return buffer;
     }

     public void deserialize(String buffer) {
        String strMessageType = buffer.substring(0, 1);
        message_type = Integer.parseInt(strMessageType);
        log.info("Got message_type" + Integer.toString(message_type));

        String strMessageSize = buffer.substring(1, 9);
        message_size = Integer.parseInt(strMessageSize);
        log.info("Got message size" + Integer.toString(message_size));
        buffer = buffer.substring(9, 9 + message_size);

        String[] dataStructs = buffer.split("\t\t\t");

        // de-serialize list of String arrays
        String[] lines = dataStructs[0].split("\t\t");
        for (String s: lines) {
            String[] word = s.split("\t");
            for (int i=0; i < word.length ; i++)
                System.out.println(word[i]);
            outQueryResult.add(word);
        }

        for (String[] arr : outQueryResult) {
            for (String s: arr) {
                log.trace(s);
           }
        }

        // de-serialize map
        String[] keyvalues = dataStructs[1].split("\t"); // check even
        for(int i = 0; i < keyvalues.length-1; i += 2) {
            outVariableMap.put(keyvalues[i], Integer.parseInt(keyvalues[i+1]));
        }

        for (Map.Entry<String, Integer> entry : outVariableMap.entrySet())
            log.trace(entry.getKey() + "\t" + entry.getValue() + "\t");
     }
    
}
