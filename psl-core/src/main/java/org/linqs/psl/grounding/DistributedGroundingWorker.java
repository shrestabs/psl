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
import java.net.InetSocketAddress;
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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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

import java.net.*;
import java.io.*;

import org.linqs.psl.grounding.messages.QueryMessage;
import org.linqs.psl.grounding.messages.ResponseMessage;
import org.linqs.psl.grounding.messages.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedGroundingWorker {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingWorker.class);
    //String serverName;
    InetSocketAddress hostAddress;
    SocketChannel master;
    boolean done = false;
    List<Rule> rules;
    AtomManager atomManager; 
    GroundRuleStore groundRuleStore;

    public DistributedGroundingWorker(String masterNodeName, List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        //this.serverName = masterNodeName;
        System.out.println("Connecting the worker to " + masterNodeName);
        this.rules = rules;
        this.atomManager = atomManager;
        this.groundRuleStore = groundRuleStore;
        hostAddress = new InetSocketAddress(masterNodeName, DistributedGroundingUtil.port);
        try {
            log.info("Connecting the worker to " + masterNodeName);
            master = SocketChannel.open(hostAddress);
        } catch (IOException e) {
            log.error("Connecting the worker to " + masterNodeName);
			e.printStackTrace();
		}
    }

    public void getQueryResult(Formula query, Constant constant, Term term, List<String[]>constList,  Map<String, Integer> newQueryVariableMap){
        QueryResultIterable queryResults = atomManager.executeGroundingQuery(query);

        int rowLength = 1; 
        for(Constant [] t : queryResults) {
            rowLength = t.length;
            String[] newRow = new String[rowLength + 1];
            for(int i = 0; i < rowLength; i++) {
                newRow[i] = t[i].rawToString();
            }
            newRow[rowLength] = constant.rawToString();

            constList.add(newRow);
            
        }
        // Iterator<Constant[]> iter = constList.iterator(); // Array Constant []
        
        Map<Variable, Integer> queryVariableMap = queryResults.getVariableMap(); 
        
        for (Map.Entry<Variable, Integer> variableMap : queryVariableMap.entrySet()) {
            newQueryVariableMap.put(variableMap.getKey().toString(), variableMap.getValue());
        }                

        newQueryVariableMap.put(term.toString(), new Integer(rowLength));
    }

    /* For every constant in the const list, that needs to be pegged, generate query  */
    // TODO JASON: Add String ConstantType as a input parameter.
    public void workerFindQueryResult(int ruleIndex, String constantString, String variable, List<String[]>constList,  Map<String, Integer> queryVariableMap){
        // Converting the constant and term from string back to their respective object.
        Constant constant = ConstantType.getConstant(constantString, ConstantType.UniqueStringID);    
        Term varTerm = new Variable(variable);

        Formula query = rules.get(ruleIndex).getRewritableGroundingFormula(atomManager);

        Database database = atomManager.getDatabase();
        Set<Atom> atoms = query.getAtoms(new HashSet<Atom>());

        List<Formula> queryAtoms = new ArrayList<Formula>();
        // Iterating through each atom in rule to variabsle to peg and create a new query. (252 - 284)
        for (Atom atom : atoms) {
            List<Term> terms = new ArrayList<Term>(); 
            Term[] atom_terms = atom.getArguments();
            Predicate predicate = atom.getPredicate();                   

            for(Term term : atom_terms) {
                // Splitting query with smallest term.
                if(term.equals(varTerm)) {
                    terms.add(constant);
                } else {
                    terms.add(term);
                }

            }
            Term[] termArray = new Term[terms.size()]; 
            termArray =  terms.toArray(termArray);
            
            QueryAtom queryAtom = new QueryAtom(predicate, termArray);
            queryAtoms.add(queryAtom);
        }
        
        
        Formula[] queryArray = new Formula[queryAtoms.size()];
        queryArray = queryAtoms.toArray(queryArray);
        System.out.println("queryAtom Size: " + queryArray[0]);

        Formula newQuery = queryArray[0];
        // If rule only contains 1 QueryAtom, just add the queryAtom otherwise create a Conjunction and then add.
        if(queryArray.length > 1) {
            newQuery = new Conjunction(queryArray);
            System.out.println(newQuery);
        }
        getQueryResult(newQuery, constant, varTerm, constList, queryVariableMap);

    }

    public void run() {
        try {
            //log.info("Connecting to " + serverName + " on port " + DistributedGroundingUtil.port);
            //Socket client = new Socket(serverName, DistributedGroundingUtil.port);
            
            // log.info("Just connected to " + client.getRemoteSocketAddress());
            // OutputStream outToServer = client.getOutputStream();
            // DataOutputStream out = new DataOutputStream(outToServer);
            
            // out.writeUTF("Hello from " + client.getLocalSocketAddress());
            // InputStream inFromServer = client.getInputStream();
            // DataInputStream in = new DataInputStream(inFromServer);

            log.info("Worker starting execution");
            while (!done) {
                ByteBuffer bytebuffer = ByteBuffer.allocate(480000);
                int bytesRead = master.read(bytebuffer);
                byte[] data = new byte[bytesRead];
                System.arraycopy(bytebuffer.array(), 0, data, 0, bytesRead);
                String buffer = new String(data);
                bytebuffer.clear();
                //String buffer = in.readUTF();
                log.info("Worker received " + buffer);
                if ((MessageType.DONE).getValue() == Integer.parseInt(buffer.substring(0, 1))) {
                    done = true;
                    log.info("Worker received " + buffer);
                    master.close();
                }
                else if ((MessageType.QUERY).getValue() == Integer.parseInt(buffer.substring(0, 1))) {
                    log.info("Worker received " + buffer);
                    QueryMessage queryMessage = new QueryMessage();
                    ResponseMessage responseMessage = new ResponseMessage();
                    queryMessage.deserialize(buffer);
                    int ruleIndex = queryMessage.inRuleIndex;
                    String variable = queryMessage.inVariableName;
                    String constant = queryMessage.inConstantValue;
                    workerFindQueryResult(ruleIndex, constant, variable, responseMessage.outQueryResult, responseMessage.outVariableMap);
                    // Prepare response message
                    String newbuffer = responseMessage.serialize();
                    ByteBuffer newbytebuffer = ByteBuffer.allocate(newbuffer.length());
                    bytebuffer.put(newbuffer.getBytes());
                    bytebuffer.flip();
                    master.write(bytebuffer);
                }
                else {
                    log.debug("Worker received " + buffer);
                    throw new IOException("Unknown message type");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
