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

/**
 * A simple response indicating if a series of query messages for that node has come to an end. 
 * Sent from Master to Worker. Worker terminates when done message is received.
 */
public class DoneMessage extends Message {
    private static final Logger log = LoggerFactory.getLogger(DoneMessage.class);
    private boolean isDone;

     public DoneMessage() {
        isDone = false;
     }

    public DoneMessage(boolean isDone) {
        this.isDone = isDone;
    }

    //TESTED
     protected String serialize() {
        String buffer = String.valueOf(isDone);
        message_size = buffer.length();
        log.info("Message size " + Integer.tostring(message_size));
        message_type = (MessageType.DONE).getValue();
        log.info("Message type " + Integer.tostring(message_type));
        buffer = Integer.toString(message_type) + String.format("%08d", message_size) + buffer; //TODO: fix size
        log.debug("Serialized {} to {}",isDone, buffer);
        return buffer;
     }

    //TESTED
     protected void deserialize(String buffer) {    
        String strMessageType = buffer.substring(0, 1);
        message_type = Integer.parseInt(strMessageType);
        log.info("Got Message type " + Integer.tostring(message_type));
        String strMessageSize = buffer.substring(1, 9);
        message_size = Integer.parseInt(strMessageSize);
        log.info("Got Message size " + Integer.tostring(message_size));
        isDone = Boolean.parseBoolean(buffer.substring(9, 9 + message_size));
        log.debug("Deserialized {} to {}", buffer, isDone);
     }

    @Override
    public String toString() {
        return "isDone: " + isDone;
    }
}
