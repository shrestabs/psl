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

public class DistributedGroundingAPI {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingAPI.class);

    public static void DistributedGroundAll (List<Rule> rules, AtomManager atomManager, GroundRuleStore groundRuleStore) {
        /** 
         * Insert entry distributed grounding code here
        */
        if (DistributedGroundingUtil.isNodeRoleMaster()) {
            log.info("Running Grounding as master node");
            DistributedGroundingMaster t = new DistributedGroundingMaster(rules, atomManager, groundRuleStore);
            t.run();
        }
        else {
            log.info("Running Grounding as slave node");
            //DistributedGroundingWorker t = new DistributedGroundingWorker(DistributedGroundingUtil.masterNodeName + DistributedGroundingUtil.DOMAIN_NAME, rules, atomManager, groundRuleStore);
            DistributedGroundingWorker t = new DistributedGroundingWorker(DistributedGroundingUtil.masterNodeName, rules, atomManager, groundRuleStore);
            t.run();
        }
        return;
    }
}

//TODO(shrbs): other functions to collect statistics 