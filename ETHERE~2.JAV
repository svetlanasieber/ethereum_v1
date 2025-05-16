/*******************************************************************************
 * Copyright (c) 2019-2024 Institute for the Architecture of Application System - University of Stuttgart
 * Author: Ghareeb Falazi
 *
 * This program and the accompanying materials are made available under the
 * terms the Apache Software License 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: Apache-2.0
 *******************************************************************************/
package blockchains.iaas.uni.stuttgart.de.plugin.ethereum;

import blockchains.iaas.uni.stuttgart.de.api.connectionprofiles.AbstractConnectionProfile;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;


@Setter
@Getter
@JsonTypeName("ethereum")
public class EthereumConnectionProfile extends AbstractConnectionProfile {
    private static final String PREFIX = "ethereum.";
    public static final String NODE_URL = PREFIX + "nodeUrl";
    public static final String KEYSTORE_PATH = PREFIX + "keystorePath";
    public static final String KEYSTORE_PASSWORD = PREFIX + "keystorePassword";
    public static final String BLOCK_TIME = PREFIX + "blockTimeSeconds";
    public static final String RMSC_ADDRESS = PREFIX + "resourceManagerSmartContractAddress";
    private String nodeUrl;
    private String keystorePath;
    private String keystorePassword;
    private int pollingTimeSeconds;
    private String resourceManagerSmartContractAddress;

    public EthereumConnectionProfile() {
    }

    public EthereumConnectionProfile(String nodeUrl, String keystorePath, String keystorePassword, int pollingTimeSeconds,
                                     String resourceManagerSmartContractAddress) {
        this.nodeUrl = nodeUrl;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.pollingTimeSeconds = pollingTimeSeconds;
        this.resourceManagerSmartContractAddress = resourceManagerSmartContractAddress;
    }

    @Override
    public Properties getAsProperties() {
        final Properties result = super.getAsProperties();
        result.setProperty(NODE_URL, this.nodeUrl);
        result.setProperty(KEYSTORE_PASSWORD, this.keystorePassword);
        result.setProperty(KEYSTORE_PATH, this.keystorePath);
        result.setProperty(BLOCK_TIME, String.valueOf(this.pollingTimeSeconds));
        result.setProperty(RMSC_ADDRESS, this.resourceManagerSmartContractAddress);

        return result;
    }

    @Override
    public Object getProperty(Object o) {

        return switch (o.toString()) {
            case NODE_URL -> this.nodeUrl;
            case KEYSTORE_PATH -> this.keystorePath;
            case KEYSTORE_PASSWORD -> this.keystorePassword;
            case BLOCK_TIME -> String.valueOf(this.pollingTimeSeconds);
            case RMSC_ADDRESS -> this.resourceManagerSmartContractAddress;
            default -> super.getAsProperties().get(o);
        };
    }

    @Override
    public void setProperty(Object o, Object o1) {
        Properties parent = super.getAsProperties();

        if (parent.containsKey(o)) {
            setAdversaryVotingRatio(Double.parseDouble(o1.toString()));
        } else {
            switch (o.toString()) {
                case NODE_URL -> this.nodeUrl = (String) o1;
                case KEYSTORE_PATH -> this.keystorePath = (String) o1;
                case KEYSTORE_PASSWORD -> this.keystorePassword = (String) o1;
                case BLOCK_TIME -> this.pollingTimeSeconds = Integer.parseInt((String) o1);
                case RMSC_ADDRESS -> this.resourceManagerSmartContractAddress = (String) o1;
            } ;
        }
    }
}
