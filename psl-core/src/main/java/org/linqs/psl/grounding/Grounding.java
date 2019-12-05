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

/**
 * Static utilities for common {@link Model}-grounding tasks.
 */
public class Grounding {
    private static final Logger log = LoggerFactory.getLogger(Grounding.class);

    public static final String CONFIG_PREFIX = "grounding";

    /**
     * Potentially rewrite the grounding queries.
     */
    public static final String REWRITE_QUERY_KEY = CONFIG_PREFIX + ".rewritequeries";
    public static final boolean REWRITE_QUERY_DEFAULT = false;

    /**
     * Whether or not queries are being rewritten, perform the grounding queries one at a time.
     */
    public static final String SERIAL_KEY = CONFIG_PREFIX + ".serial";
    public static final boolean SERIAL_DEFAULT = false;

    // Static only.
    private Grounding() {}

    /**
     * Ground all the given rules.
     * @return the number of ground rules generated.
     */
    public static int groundAll(Model model, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        return groundAll(model.getRules(), atomManager, groundRuleStore);
    }

    /**
     * Ground all the given rules one at a time.
     * Callers should prefer groundAll() to this since it will perform a more efficient grounding.
     * @return the number of ground rules generated.
     */
    public static int groundAllSerial(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        int groundCount = 0;
        for (Rule rule : rules) {
            groundCount += rule.groundAll(atomManager, groundRuleStore);
        }

        return groundCount;
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

    public static Constant stringToConstant(String constant_string, String constantType) {

        String constString = constant.rawToString();
        Constant newConstant = ConstantType.getConstant(constString, constantType);
        return newConstant
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
  
    public void workerFindQueryResult(int ruleIndex, String constantString, String variable, String constantType){
        // Converting the constant and term from string back to their respective object.
        Constant constant = ConstantType.getConstant(constantString, constantType);    
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
 
    public void orgainzeJobsForWorker(int ruleIndex, HashSet<Constant> constants, Term variable) {
          String variableString = variable.toString();
          for(Constant constant : constants) {
              ConstantType constantType = ConstantType.getType(constant); // This should be a String to pass to worker.
              String constantString = constant;
           
              workerFindQueryResult(ruleIndex, constantString, variableString);
  //              groundingSubQuery(newQuery, tempRules, atomManager, groundRuleStore, constant, smallestTerm); 
        }
    }


    public void groundDistributed(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        
        Map<Term, HashSet<Constant>> predicateConstants = new HashMap<Term, HashSet<Constant>>();
        Map<Term, List<String>> newPredicateConstants = new HashMap<Term, List<String>>();
        
        int num_rules = rules.size()
        // Obtaining the index of each rule list.
        for (int rule_index = 0; rule_index < num_rules; rule_index++) {
            Formula query = rules[rule_index].getRewritableGroundingFormula(atomManager);

            Database database = atomManager.getDatabase();
            Set<Atom> atoms = query.getAtoms(new HashSet<Atom>());
            
            predicateConstants = findTermConstant(database, atoms);
            Term smallestTerm = findSmallestTerm(predicateConstants);

            List <Constant[]> outQueryResult = new ArrayList<Constant[]>();
            Map<Variable, Integer> outVariableMap = new HashMap<Variable, Integer>();

            DistributedGroundAll (rule_index, smallestTerm, predicateConstants.get(smallestTerm), outQueryResult, outVariableMap);
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
    }

    


    /**
     * Ground all the given rules.
     * @return the number of ground rules generated.
     */
    public static int groundAll(List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        boolean rewrite = Config.getBoolean(REWRITE_QUERY_KEY, REWRITE_QUERY_DEFAULT);
        boolean serial = Config.getBoolean(SERIAL_KEY, SERIAL_DEFAULT);
        int initialSize = groundRuleStore.size();

        Map<Formula, List<Rule>> queries = new HashMap<Formula, List<Rule>>();
        List<Rule> bypassRules = new ArrayList<Rule>();

        DataStore dataStore = atomManager.getDatabase().getDataStore();
        if (rewrite && !(dataStore instanceof RDBMSDataStore)) {
            log.warn("Cannot rewrite queries with a non-RDBMS DataStore. Queries will not be rewritten.");
            rewrite = false;
        }

        QueryRewriter rewriter = null;
        if (rewrite) {
            rewriter = new QueryRewriter();
        }

        for (Rule rule : rules) {
            if (!rule.supportsGroundingQueryRewriting()) {
                bypassRules.add(rule);
                continue;
            }

            Formula query = rule.getRewritableGroundingFormula(atomManager);
            if (rewrite) {
                query = rewriter.rewrite(query, (RDBMSDataStore)dataStore);
            }

            if (!queries.containsKey(query)) {
                queries.put(query, new ArrayList<Rule>());
            }
            queries.get(query).add(rule);
        }
        // First perform all the rewritten querties.
        for (Map.Entry<Formula, List<Rule>> entry : queries.entrySet()) {
            if (!serial) {
                // If parallel, ground all the rules that match this formula at once.
                groundParallel(entry.getKey(), entry.getValue(), atomManager, groundRuleStore);
            } else {
                // If serial, ground the rules with this formula one at a time.
                for (Rule rule : entry.getValue()) {
                    List<Rule> tempRules = new ArrayList<Rule>();
                    tempRules.add(rule);

                    groundParallel(entry.getKey(), tempRules, atomManager, groundRuleStore);
                }
            }
        }
        // Now ground the bypassed rules.
        groundAllSerial(bypassRules, atomManager, groundRuleStore);

        return groundRuleStore.size() - initialSize;
    }

    // Grounding the created subqueries
    private static int groundingSubQuery(Formula query, List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore, Constant constant, Term term) {
            log.debug("Grounding {} rule(s) with query: [{}].", rules.size(), query);
            for (Rule rule : rules) {
                log.trace("    " + rule);
            }
            // Grounding the new query.
            int initialCount = groundRuleStore.size();
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


            // DONT LOOK PAST HERE SHRESTA.
            System.out.println("QUERY VAR MAP: " + newQueryVariableMap);        
            Parallel.RunTimings timings = Parallel.foreach(iter, new GroundWorker(atomManager, groundRuleStore, newQueryVariableMap, rules));                   
            int groundCount = groundRuleStore.size() - initialCount;
            log.trace("Got {} results from query [{}].", timings.iterations, query);
            log.debug("Generated {} ground rules with query: [{}].", groundCount, query);

            return groundCount;
    }

    private static int groundParallel(Formula query, List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        log.debug("Grounding {} rule(s) with query: [{}].", rules.size(), query);
        for (Rule rule : rules) {
            log.trace("    " + rule);
        }

        // We will manually handle these in the grounding process.
        // We do not want to throw too early because the ground rule may turn out to be trivial in the end.
        boolean oldAccessExceptionState = atomManager.enableAccessExceptions(false);

        int initialCount = groundRuleStore.size();
        QueryResultIterable queryResults = atomManager.executeGroundingQuery(query);
        Parallel.RunTimings timings = Parallel.foreach(queryResults, new GroundWorker(atomManager, groundRuleStore, queryResults.getVariableMap(), rules));
        int groundCount = groundRuleStore.size() - initialCount;

        atomManager.enableAccessExceptions(oldAccessExceptionState);

        log.trace("Got {} results from query [{}].", timings.iterations, query);
        log.debug("Generated {} ground rules with query: [{}].", groundCount, query);
        return groundCount;
    }

    private static class GroundWorker extends Parallel.Worker<Constant[]> {
        private AtomManager atomManager;
        private GroundRuleStore groundRuleStore;
        private Map<Variable, Integer> variableMap;
        private List<Rule> rules;
        private List<GroundRule> groundRules;

        public GroundWorker(AtomManager atomManager, GroundRuleStore groundRuleStore,
                Map<Variable, Integer> variableMap, List<Rule> rules) {
            this.atomManager = atomManager;
            this.groundRuleStore = groundRuleStore;
            this.variableMap = variableMap;
            this.rules = rules;
            this.groundRules = new ArrayList<GroundRule>();
        }

        @Override
        public Object clone() {
            return new GroundWorker(atomManager, groundRuleStore, variableMap, rules);
        }

        @Override
        public void work(int index, Constant[] row) {
            for (Rule rule : rules) {
                rule.ground(row, variableMap, atomManager, groundRules);

                for (GroundRule groundRule : groundRules) {
                    if (groundRule != null) {
                        groundRuleStore.addGroundRule(groundRule);
                    }
                }

                groundRules.clear();
            }
        }
    }
    
    public static Map<Term, List<String>> newFindTermConstant(Database database, Set<Atom> atoms) {
        Map<Term, List<String>> predicateConstants = new HashMap<Term, List<String>>();

        // TODO Jason: This should become a function where you return all the queries
        for (Atom atom : atoms) {
            Predicate pred = atom.getPredicate();
    
            if(pred instanceof StandardPredicate){
                System.out.println("Pred Class: " +pred.getClass());
                // System.out.println(j.getAllGroundObservedAtoms((StandardPredicate) pred));
                List<ObservedAtom> groundObservedAtoms = database.getAllGroundObservedAtoms((StandardPredicate) pred);

                for(ObservedAtom obsAtom : groundObservedAtoms){
                    GroundAtom grAtom = (GroundAtom) obsAtom;
                    Constant[] constants = obsAtom.getArguments();
                    Term[] terms = atom.getArguments();
                    for( int i = 0; i < constants.length; i++){
                        Constant atom_constant = constants[i];
                        Term atom_term = terms[i];
                        
                        // If the Hashmap doesn't have a term as the key add it to the Hashmap.
                        if (!predicateConstants.containsKey(atom_term)) {
                            predicateConstants.put(atom_term, new ArrayList<String>());
                        }
                        predicateConstants.get(atom_term).add(atom_constant.rawToString());
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
    public static Term newFindSmallestTerm(Map<Term, List<String>> termConstants) {
        Term smallestTerm = null;
        double numConstants = Double.POSITIVE_INFINITY;
        for (Map.Entry<Term, List<String>> entry : termConstants.entrySet()) {
            Term termInQuestion = entry.getKey();
            List<String> constant = entry.getValue();
            double constantSize = constant.size();
        
            if (constantSize < numConstants) {
                numConstants = constantSize;
                smallestTerm = termInQuestion;
            } 
        }
        System.out.println("Num Constants: " + numConstants);
        return smallestTerm;
    }

}
