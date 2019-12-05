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

    public void getQueryResult(Formula query, AtomManager atomManager, Constant constant, Term term){
        QueryResultIterable queryResults = atomManager.executeGroundingQuery(query);
        List<Constant[]> constList = new ArrayList<Constant[]>(); // You can probably just return this.

        int rowLength = 1; 
        for(Constant [] t : queryResults) {
            rowLength = t.length;
            Constant [] newRow = new Constant[rowLength + 1];
            for(int i = 0; i < rowLength; i++) {
                newRow[i] = t[i];
            }
            newRow[rowLength] = constant;

            constList.add(newRow);
            
        }
        Iterator<Constant[]> iter = constList.iterator(); // Array Constant []
        
        // Obtaining the variable mapping of variable to index (int) of constant []. Line (359 - 367) Creating the variable map
        Map<Variable, Integer> queryVariableMap = queryResults.getVariableMap(); 
        Map<Variable, Integer> newQueryVariableMap = new HashMap<Variable, Integer>(); // You can just return this to me.
        
        for (Map.Entry<Variable, Integer> variableMap : queryVariableMap.entrySet()) {
            newQueryVariableMap.put(variableMap.getKey(), variableMap.getValue());
        }                

        newQueryVariableMap.put((Variable) term, new Integer(rowLength));
    }

    /* For every constant in the const list, that needs to be pegged, generate query */
    public void workerFindQueryResult(int ruleIndex, String constantString, String variable, String constantType){
        // Converting the constant and term from string back to their respective object.
        Constant constant = ConstantType.getConstant(constantString, "UniqueStringID");    
        Term term = new Variable(variable);

        // TODO: Somehow find the list of rules.
        Formula query = rules[ruleIndex].getRewritableGroundingFormula(atomManager);

        Database database = atomManager.getDatabase();
        Set<Atom> atoms = query.getAtoms(new HashSet<Atom>());

        List<Formula> queryAtoms = new ArrayList<Formula>();
        // Iterating through each atom in rule to variable to peg and create a new query. (252 - 284)
        for (Atom atom : atoms) {
            List<Term> terms = new ArrayList<Term>(); 
            Term[] atom_terms = atom.getArguments();
            Predicate predicate = atom.getPredicate();                   

            for(Term term : atom_terms) {
                // Splitting query with smallest term.
                if(term.equals(smallestTerm)) {
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
    }

    public void run(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
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
                    log.debug("Worker received " + buffer);
                    throw new IOException("Unknown message type");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}