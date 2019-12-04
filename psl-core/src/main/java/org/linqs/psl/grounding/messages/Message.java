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

public abstract class Message {
    public enum MessageType {
        QUERY(1), 
        RESPONSE(2), 
        DONE(3);
    
        private final int value;
        private MessageType(int value) {
            this.value = value;
        }
    
        public int getValue() {
            return value;
        }
    }
    int message_type;
    // ignore commmon fields like encoding
    int message_size_length = 8; //TODO: fix size
    int message_size;

    // Serialize string to be passed on the wire
    protected abstract String serialize();

    // Deserialize buffer into the Message object
    protected abstract void deserialize(String buffer);
}