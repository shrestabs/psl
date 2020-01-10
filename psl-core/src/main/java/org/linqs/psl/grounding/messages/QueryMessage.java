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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMessage extends Message {
    private static final Logger log = LoggerFactory.getLogger(QueryMessage.class);
    String messagename = "Query Message";
    public int inRuleIndex;
    public String inVariableName;
    public String inConstantValue;

    public QueryMessage() {
        this.inRuleIndex = 0;
        this.inVariableName = "";
        this.inConstantValue = "";
    }

    public String serialize() {
        String buffer;
        buffer = Integer.toString(this.inRuleIndex) + "," + this.inVariableName + "," + this.inConstantValue;
        message_size = buffer.length();
        log.info("Added msg size " + Integer.toString(message_size));
        message_type = (MessageType.QUERY).getValue();
        log.info("Added msg type " + Integer.toString(message_type));
        buffer = Integer.toString(message_type) + String.format("%08d", message_size) + buffer;
        log.info("Serialized {}, {}, {} to {}", this.inRuleIndex, this.inVariableName, this.inConstantValue, buffer);
        return buffer;
    }

    public void deserialize(String buffer) {
        String strMessageType = buffer.substring(0, 1);
        message_type = Integer.parseInt(strMessageType);
        log.info("Got msg type " + Integer.toString(message_type));
        String strMessageSize = buffer.substring(1, 9);
        message_size = Integer.parseInt(strMessageSize);
        log.info("Got msg size " + Integer.toString(message_size));
        buffer = buffer.substring(9, 9 + message_size);
        String[] values = buffer.split(",");
        this.inRuleIndex = Integer.parseInt(values[0]);
        this.inVariableName = values[1];
        this.inConstantValue = values[2];
        log.info("Deserialized {} to {}, {}, {}", buffer, this.inRuleIndex, this.inVariableName, this.inConstantValue);
    }

    // @Override
    // public String toString() {
    //     return messagename;
    // }

}
