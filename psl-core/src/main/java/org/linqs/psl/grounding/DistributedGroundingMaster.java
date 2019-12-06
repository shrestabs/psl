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

import java.net.*;
import java.io.*;
import java.nio.channels.Selector;
import java.util.List;


public class DistributedGroundingMaster {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingMaster.class);
    private static boolean groundingStatus = true;
    private ServerSocket serverSocket;
    private final int port = 6066;
    List<Rule> rules;
    AtomManager atomManager; 
    GroundRuleStore groundRuleStore;
   
    public DistributedGroundingMaster(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        this.rules = rules;
        this.atomManager = atomManager;
        this.groundRuleStore = groundRuleStore;
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
     * Create a loop of constants that will be used for pegging 
    */
    public void orgainzeJobsForWorker(int ruleIndex, HashSet<Constant> constants, Term variable) {
        String variableString = variable.toString();
        for(Constant constant : constants) {
            ConstantType constantType = ConstantType.getType(constant); // This should be a String to pass to worker.
            String constantString = constant;
         
            workerFindQueryResult(ruleIndex, constantString, variableString);
            /* call send msg and serialize */ 
            // groundingSubQuery(newQuery, tempRules, atomManager, groundRuleStore, constant, smallestTerm); 
        }
    }

    public void run() {
          try {
             log.info("Waiting for slaves on port " + 
                serverSocket.getLocalPort() + " to come online...");
            
            // event multiplxer 
            Selector selector = Selector.open();
            
             Socket server = serverSocket.accept();
             
             System.out.println("Just connected to " + server.getRemoteSocketAddress());
             DataInputStream in = new DataInputStream(server.getInputStream());
             
             Map<Term, HashSet<Constant>> predicateConstants = new HashMap<Term, HashSet<Constant>>();
             Map<Term, List<String>> newPredicateConstants = new HashMap<Term, List<String>>();
             
             int num_rules = rules.size();
             // Obtaining the index of each rule list.
             for (int rule_index = 0; rule_index < num_rules; rule_index++) {
                 Formula query = rules[rule_index].getRewritableGroundingFormula(atomManager);
     
                 Database database = atomManager.getDatabase();
                 Set<Atom> atoms = query.getAtoms(new HashSet<Atom>());
                 
                 predicateConstants = findTermConstant(database, atoms);
                 Term smallestTerm = findSmallestTerm(predicateConstants);
     
                 List <Constant[]> outQueryResult = new ArrayList<Constant[]>();
                 Map<Variable, Integer> outVariableMap = new HashMap<Variable, Integer>();
            // make indices  (i, list, variable)
            orgainzeJobsForWorker(inRuleIndex, inVariableName, inConstantValue);
            // make msg packet to slave i , list[j], var
            System.out.println(in.readUTF());
            // wait for all slaves
            // done msg to all slaves
                 //create message
                 //DistributedGroundAll (rule_index, smallestTerm, predicateConstants.get(smallestTerm), outQueryResult, outVariableMap);
                 List<GroundRule> groundRules = new ArrayList<GroundRule>();
                 for (Constant [] row : outQueryResult) {
                     rule.ground(row, outVariableMap, atomManager, groundRules);
                     for (GroundRule groundRule : groundRules) {
                         if (groundRule != null) {
                             groundRuleStore.addGroundRule(groundRule);
                         }
                     }
                     groundRules.clear();
                 }
             }
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