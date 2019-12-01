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

public class DistributedGroundingMaster extends Thread {
    private ServerSocket serverSocket;
   
    public DistributedGroundingMaster(int port) throws IOException {
       serverSocket = new ServerSocket(port);
       serverSocket.setSoTimeout(10000);
    }
 
    public void run() {
       while(true) {
          try {
             System.out.println("Waiting for client on port " + 
                serverSocket.getLocalPort() + "...");
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
    
    public static void runMaster(String [] args) {
       int port = Integer.parseInt(args[0]);
       try {
          Thread t = new DistributedGroundingMaster(port);
          t.start();
       } catch (IOException e) {
          e.printStackTrace();
       }
    }
}