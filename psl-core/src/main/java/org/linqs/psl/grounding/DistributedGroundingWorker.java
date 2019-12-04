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

import java.net.*;
import java.io.*;

import org.linqs.psl.grounding.messages.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedGroundingWorker {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingWorker.class);
    String serverName;
    final int port = 6066;
    boolean done = false;

    public DistributedGroundingWorker(String masterNodeName) {
        serverName = masterNodeName;
    }

    public void run() {
        try {
            log.info("Connecting to " + serverName + " on port " + port);
            Socket client = new Socket(serverName, port);
            
            log.info("Just connected to " + client.getRemoteSocketAddress());
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            
            out.writeUTF("Hello from " + client.getLocalSocketAddress());
            InputStream inFromServer = client.getInputStream();
            DataInputStream in = new DataInputStream(inFromServer);
            
            while (!done) {
                String buffer = in.readUTF();
                log.debug("Worker received " + buffer);
                if ((MessageType.DONE).getValue() == Integer.parseInt(buffer.substring(0, 1))) {
                    log.debug("Worker received " + buffer);
                    client.close();
                }
                else if ((MessageType.QUERY).getValue() == Integer.parseInt(buffer.substring(0, 1))) {
                    log.debug("Worker received " + buffer);
                    // Prepare query message
                }
                else {
                    log.debug("Worker received " + buffer);w
                    throw new IOException("Unknown message type");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}