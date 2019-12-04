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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.Variable;

public class DistributedGroundingAPI {
    private static final Logger log = LoggerFactory.getLogger(DistributedGroundingAPI.class);

    public void DistributedGroundAll (int inRuleIndex, String inVariableName, List <String> inConstantValue, List <Constant[]> outQueryResult, Map<Variable, Integer> outVariableMap) {
        /** 
         * Insert entry distributed grounding code here
        */
        if (DistributedGroundingUtil.isNodeRoleMaster()) {
            log.info("Running Grounding as master node");
            DistributedGroundingMaster t = new DistributedGroundingMaster();
            t.run(inRuleIndex, inVariableName, inConstantValue);
        }
        else {
            log.info("Running Grounding as slave node");
            DistributedGroundingWorker t = new DistributedGroundingWorker(DistributedGroundingUtil.masterNodeName + DistributedGroundingUtil.DOMAIN_NAME);
            t.run(outQueryResult, outVariableMap);
        }
        return;
    }

}

//TODO(shrbs): other functions to collect statistics 