/*******************************************************************************
 * Copyright (c) 2023-2024 Institute for the Architecture of Application System - University of Stuttgart
 * Author: Ghareeb Falazi
 *
 * This program and the accompanying materials are made available under the
 * terms the Apache Software License 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: Apache-2.0
 *******************************************************************************/
package blockchains.iaas.uni.stuttgart.de.plugin.ethereum;

import blockchains.iaas.uni.stuttgart.de.api.model.ResourceManagerSmartContract;
import blockchains.iaas.uni.stuttgart.de.api.model.SmartContractEvent;
import blockchains.iaas.uni.stuttgart.de.api.model.SmartContractFunction;

import java.util.List;

public class EthereumResourceManagerSmartContract extends ResourceManagerSmartContract {
    @Override
    public SmartContractEvent getAbortEvent() {
        return getEvents().stream().filter(e->e.getFunctionIdentifier().equals("TxAborted")).findFirst().orElse(null);
    }

    @Override
    public SmartContractEvent getVoteEvent() {
        return getEvents().stream().filter(e->e.getFunctionIdentifier().equals("Voted")).findFirst().orElse(null);
    }

    @Override
    public SmartContractFunction getPrepareFunction() {
        return getFunctions().stream().filter(f -> f.getFunctionIdentifier().equals("prepare")).findFirst().orElse(null);
    }

    @Override
    public SmartContractFunction getAbortFunction() {
        return getFunctions().stream().filter(f -> f.getFunctionIdentifier().equals("abort")).findFirst().orElse(null);
    }

    @Override
    public SmartContractFunction getCommitFunction() {
        return getFunctions().stream().filter(f -> f.getFunctionIdentifier().equals("commit")).findFirst().orElse(null);
    }

    public EthereumResourceManagerSmartContract(String smartContractPath, List<SmartContractFunction> functions, List<SmartContractEvent> events) {
        super(smartContractPath, functions, events);
    }
}
