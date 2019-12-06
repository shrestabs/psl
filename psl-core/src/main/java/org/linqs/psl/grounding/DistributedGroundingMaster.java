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
import org.netlib.util.booleanW;

import java.net.InetSocketAddress;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Integer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.net.*;
import java.io.*;

import org.linqs.psl.grounding.messages.QueryMessage;
import org.linqs.psl.grounding.messages.ResponseMessage;
import org.linqs.psl.grounding.messages.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.io.*;
import java.nio.channels.Selector;
import java.util.List;


public class DistributedGroundingMaster {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingMaster.class);
    private static boolean groundingStatus = true;
    private Selector selector;
    private InetSocketAddress listenAddress;
    //private ServerSocket serverSocket;
    int worksConnected;
    int totalWorkers;
    private final int port = 6066;
    List<Rule> rules;
    boolean ruleNotDone;
    AtomManager atomManager; 
    GroundRuleStore groundRuleStore;
    HashMap<String, Boolean> workerStatus = new HashMap<>(); 
    Map<Variable, Integer> goldStandardOutVariableMap = new HashMap <Variable,Integer>();
   
    public DistributedGroundingMaster(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        this.rules = rules;
        this.atomManager = atomManager;
        this.groundRuleStore = groundRuleStore;
        this.worksConnected = 0;
        this.ruleNotDone = true;
        this.totalWorkers = DistributedGroundingUtil.slaveNodeNameList.size();
        listenAddress = new InetSocketAddress(DistributedGroundingUtil.masterNodeName, DistributedGroundingUtil.port);
        //listenAddress = new InetSocketAddress(DistributedGroundingUtil.masterNodeName + DistributedGroundingUtil.DOMAIN_NAME, DistributedGroundingUtil.port);
        //TODO figure which workers are online 
        for (String worker : DistributedGroundingUtil.slaveNodeNameList) {
            workerStatus.put(worker, false);
        }

        // try {
        //     serverSocket = new ServerSocket(port);
        //     //serverSocket.setSoTimeout(10000);
        // }
        // catch (IOException e) {
        //     e.printStackTrace();
        // }
    }

    public static boolean getGroundingStatus () {
        return groundingStatus;
    }

    public static void setGroundingStatus (boolean status) {
        groundingStatus = status;
    }

    /**
     * Obtain a all the constant corresponding to the term.
     * @return a HashMap of Term to Constant set. 
     */
    public static Map<Term, HashSet<Constant>> findTermConstant(Database database, Set<Atom> atoms) {
        Map<Term, HashSet<Constant>> predicateConstants = new HashMap<Term, HashSet<Constant>>();

        for (Atom temp: atoms) {
            System.out.println("Atom Arguments: " + temp.getArguments().getClass());
            Predicate pred = temp.getPredicate();
    
            if(pred instanceof StandardPredicate){
                System.out.println("Pred Class: " +pred.getClass());
                List<ObservedAtom> groundObservedAtoms = database.getAllGroundObservedAtoms((StandardPredicate) pred);

                for(ObservedAtom obsAtom : groundObservedAtoms){
                    GroundAtom grAtom = (GroundAtom) obsAtom;
                    Constant[] constants = obsAtom.getArguments();
                    Term[] terms = temp.getArguments();
                    for( int i = 0; i < constants.length; i++){
                        Constant atom_constant = constants[i];
                        Term atom_term = terms[i];
                        
                        // If the Hashmap doesn't have a term as the key add it to the Hashmap.
                        if (!predicateConstants.containsKey(atom_term)) {
                            predicateConstants.put(atom_term, new HashSet<Constant>());
                        }
                        predicateConstants.get(atom_term).add(atom_constant);
                    }
                }
            }
        }
        return predicateConstants;
    } 

    /**
     * Given a Hashmap of term to constant, find term with the smallest number of constants.
     * @return the term with smallest number of constants
     */
    public static Term findSmallestTerm(Map<Term, HashSet<Constant>> termConstants) {
        Term smallestTerm = null;
        double numConstants = Double.POSITIVE_INFINITY;
        for (Map.Entry<Term, HashSet<Constant>> entry : termConstants.entrySet()) {
            Term termInQuestion = entry.getKey();
            Set<Constant> constant = entry.getValue();
            double constantSize = constant.size();
        
            if (constantSize < numConstants) {
                numConstants = constantSize;
                smallestTerm = termInQuestion;
            } 
        }
        System.out.println("Num Constants: " + numConstants);
        return smallestTerm;
    }

    /*
    * Finds the next free worker in the worker list 
    */
    private String findNextFreeWorker() {
        Iterator<Map.Entry<String, Boolean>> itr = workerStatus.entrySet().iterator(); 
        while (itr.hasNext()) {
            Map.Entry<String, Boolean> entry = itr.next();
            if (entry.getValue() == true) {
                log.info("Next free worker " + entry.getKey());
                return entry.getKey();
            }
        }
        log.error(" slave Not found");
        return "";
    }

    // accept client connection
    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        Socket socket = channel.socket();
        SocketAddress remoteAddr = socket.getRemoteSocketAddress();
        System.out.println("Connected to: " + remoteAddr);
        worksConnected = worksConnected + 1;

        /*
         * Register channel with selector for further IO (record it for read/write
         * operations, here we have used read operation)
         */
        channel.register(this.selector, SelectionKey.OP_READ);
    }

    // write from the socket channel
    private void write(String slaveNodeName, String stringbuffer) throws IOException {
        InetSocketAddress hostAddress = new InetSocketAddress(slaveNodeName + DistributedGroundingUtil.DOMAIN_NAME, 6066);
        SocketChannel worker = SocketChannel.open(hostAddress);
        ByteBuffer bytebuffer = ByteBuffer.allocate(stringbuffer.length());
        bytebuffer.put(stringbuffer.getBytes());
        bytebuffer.flip();
        worker.write(bytebuffer);
    }

    // read from the socket channel
    private String read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024000);
        int numRead = -1;
        numRead = channel.read(buffer);
        //create response message 

        if (numRead == -1) {
            Socket socket = channel.socket();
            SocketAddress remoteAddr = socket.getRemoteSocketAddress();
            System.out.println("Connection closed by client: " + remoteAddr);
            channel.close();
            key.cancel();
            return "";
        }

        byte[] data = new byte[numRead];
        System.arraycopy(buffer.array(), 0, data, 0, numRead);
        return new String(data);
    }



    public void run() {
          try {
             //log.info("Waiting for slaves on port " + serverSocket.getLocalPort() + " to come online...");
            // Selector for server 
            this.selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(listenAddress);
            log.info("Bind to listen address " + listenAddress);
            serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);
            log.info("Role: Master, running on port " + 6066); //TODO: remove hardcode

            // accept connections
            while (worksConnected < totalWorkers) {
                // wait for events
                int readyCount = selector.select();
                if (readyCount == 0) {
                    continue;
                }
    
                // process selected keys...
                Set<SelectionKey> readyKeys = selector.selectedKeys();
                Iterator iterator = readyKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = (SelectionKey) iterator.next();
    
                    // Remove key from set so we don't process it twice
                    iterator.remove();
    
                    if (!key.isValid()) {
                        continue;
                    }
    
                    if (key.isAcceptable()) { // Accept client connections
                        this.accept(key);
                    }
                }
            }

            log.info("All workers connected!");

            int num_rules = rules.size();
            // Obtaining the index of each rule list.
            for (int rule_index = 0; rule_index < num_rules; rule_index++) {
               Formula query = rules.get(rule_index).getRewritableGroundingFormula(atomManager);
               Database database = atomManager.getDatabase();
               Set<Atom> atoms = query.getAtoms(new HashSet<Atom>());
               Map<Term, HashSet<Constant>> predicateConstants = new HashMap<Term, HashSet<Constant>>();
               predicateConstants = findTermConstant(database, atoms);
               Term smallestTerm = findSmallestTerm(predicateConstants);
                HashSet<Constant> constantSet = new HashSet<Constant>();
                constantSet = predicateConstants.get(smallestTerm);
                List<Constant> constantList = new ArrayList<Constant>(constantSet); 

                List <Constant []> outQueryResult = new ArrayList<Constant []>();
                Map<Variable, Integer> outVariableMap = new HashMap <Variable,Integer>();

               String variableString = smallestTerm.toString();
               int nextConstantToSend = 0;
               int totalNumberOfConstants = constantList.size();
               System.out.println("totalNumberOfConstants = " + Integer.toString(totalNumberOfConstants));
               int nextWorkerToSend = 0;
                while (ruleNotDone) {
                    // wait for responses events
                    int readyCount = selector.select();
                    if (readyCount == 0) {
                        continue;
                    }
                    // process selected keys...
                    Set<SelectionKey>  readyKeys = selector.selectedKeys();
                    Iterator iterator = readyKeys.iterator();

                    while (iterator.hasNext()) {
                        SelectionKey key = (SelectionKey) iterator.next();
        
                        // Remove key from set so we don't process it twice
                        iterator.remove();
        
                        if (!key.isValid()) {
                            continue;
                        }
                        // if we got response message process it
                        if (key.isReadable()) {
                            log.info("Got a response from worker");
                            ResponseMessage responseMessage = new ResponseMessage();
                            String buffer = this.read(key);
                            responseMessage.deserialize(buffer);
                            Map<Variable, Integer> workerOutVariableMap = DistributedGroundingUtil.stringMapToVariableMap(responseMessage.outVariableMap);
                            if (goldStandardOutVariableMap.isEmpty()) {
                                goldStandardOutVariableMap = workerOutVariableMap;
                            }
                            List<String []> reorderedOutQueryResult = DistributedGroundingUtil.reorderArray(goldStandardOutVariableMap, workerOutVariableMap, responseMessage.outQueryResult);

                            for (String [] item : reorderedOutQueryResult) {
                                outQueryResult.add(DistributedGroundingUtil.stringArrayToConstant(item));
                            }
                        }
                    }

                    System.out.println("finding next free worker");
                    String slaveNodeName = findNextFreeWorker();
                    if (nextConstantToSend < totalNumberOfConstants) {
                        // now attempt sending query messages
                        QueryMessage queryMessage = new QueryMessage();
                        queryMessage.inRuleIndex = rule_index;
                        queryMessage.inVariableName = variableString;
                        //ConstantType constantType = ConstantType.getType(constantList[]); // This should be a String to pass to worker.
                        String constantString = constantList.get(nextConstantToSend).rawToString();
                        queryMessage.inConstantValue = constantString;
                        log.info("Sending sub-query " + constantString);
                        String buffer = queryMessage.serialize();
                        this.write(slaveNodeName, buffer);
                        nextConstantToSend = nextConstantToSend + 1;
                    }
                    if (nextConstantToSend >= totalNumberOfConstants) {
                        ruleNotDone = false;
                    }
                } // while (ruleNotDone)

                List<GroundRule> groundRules = new ArrayList<GroundRule>();
                for (Constant [] row : outQueryResult) {
                    rules.get(rule_index).ground(row, goldStandardOutVariableMap, atomManager, groundRules);
                    for (GroundRule groundRule : groundRules) {
                        if (groundRule != null) {
                            groundRuleStore.addGroundRule(groundRule);
                        }
                    }
                    groundRules.clear();
                }
                goldStandardOutVariableMap.clear();

            } // for (int rule_index = 0; rule_index < num_rules; rule_index++)
    
        } catch (IOException e) {
            e.printStackTrace();
      }
    }
}