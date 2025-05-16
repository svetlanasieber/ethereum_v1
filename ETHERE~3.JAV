/*******************************************************************************
 * Copyright (c) 2022-2024 Institute for the Architecture of Application System - University of Stuttgart
 * Author: Akshay Patel
 * Co-Author: Ghareeb Falazi
 *
 * This program and the accompanying materials are made available under the
 * terms the Apache Software License 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: Apache-2.0
 *******************************************************************************/
package blockchains.iaas.uni.stuttgart.de.plugin.ethereum;

import blockchains.iaas.uni.stuttgart.de.api.IAdapterExtension;
import blockchains.iaas.uni.stuttgart.de.api.connectionprofiles.AbstractConnectionProfile;
import blockchains.iaas.uni.stuttgart.de.api.interfaces.BlockchainAdapter;
import blockchains.iaas.uni.stuttgart.de.api.utils.PoWConfidenceCalculator;
import lombok.extern.log4j.Log4j2;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.web3j.crypto.exception.CipherException;

import java.io.IOException;

@Log4j2
public class EthereumPlugin extends Plugin {
    public EthereumPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Extension
    @Log4j2
    public static class EthAdapterImpl implements IAdapterExtension {

        @Override
        public BlockchainAdapter getAdapter(AbstractConnectionProfile connectionProfile) {
            assert connectionProfile instanceof EthereumConnectionProfile;
            EthereumConnectionProfile ethereumConnectionProfile = (EthereumConnectionProfile) connectionProfile;
            String nodeUrl = ethereumConnectionProfile.getNodeUrl();
            int pollingTimeSeconds = ethereumConnectionProfile.getPollingTimeSeconds();
            double adversaryVotingRatio = ethereumConnectionProfile.getAdversaryVotingRatio();
            String keystorePassword = ethereumConnectionProfile.getKeystorePassword();
            String keystorePath = ethereumConnectionProfile.getKeystorePath();
            String resourceManagerSmartContractAddress = ethereumConnectionProfile.getResourceManagerSmartContractAddress();

            final EthereumAdapter adapter = new EthereumAdapter(nodeUrl, pollingTimeSeconds, resourceManagerSmartContractAddress);

            final PoWConfidenceCalculator cCalc = new PoWConfidenceCalculator();
            cCalc.setAdversaryRatio(adversaryVotingRatio);

            try {
                adapter.setCredentials(keystorePassword, keystorePath);
            } catch (IOException | CipherException e) {
                log.warn("Failed to set credentials for the Ethereum Adapter of the connection profile: {}", connectionProfile, e);
            }

            adapter.setConfidenceCalculator(cCalc);

            return adapter;
        }

        @Override
        public Class<? extends AbstractConnectionProfile> getConnectionProfileClass() {
            return EthereumConnectionProfile.class;
        }

        @Override
        public String getConnectionProfileNamedType() {
            return "ethereum";
        }

        @Override
        public String getBlockChainId() {
            return "ethereum";
        }

    }
}
