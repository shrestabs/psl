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
import java.nio.channels.Selector;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedGroundingMaster {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingMaster.class);
    private static boolean groundingStatus = true;
    private ServerSocket serverSocket;
    private final int port = 6066;
   
    public DistributedGroundingMaster() {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(10000);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean getGroundingStatus () {
        return groundingStatus;
    }

    public static void setGroundingStatus (boolean status) {
        groundingStatus = status;
    }

    public void run(int inRuleIndex, String inVariableName, List <String> inConstantValue) {
       while(groundingStatus) {
          try {
             log.info("Waiting for slaves on port " + 
                serverSocket.getLocalPort() + " to come online...");
            
            Selector selector = Selector.open();
            
             Socket server = serverSocket.accept();
             
             System.out.println("Just connected to " + server.getRemoteSocketAddress());
             DataInputStream in = new DataInputStream(server.getInputStream());
             
             System.out.println(in.readUTF());
             DataOutputStream out = new DataOutputStream(server.getOutputStream());
             out.writeUTF("Thank you for connecting to " + server.getLocalSocketAddress()
                + "\nGoodbye!");
             server.close();
             
          } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
          } catch (IOException e) {
                e.printStackTrace();
             break;
          }
       }
    }
}