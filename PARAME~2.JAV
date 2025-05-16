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

import blockchains.iaas.uni.stuttgart.de.api.exceptions.ParameterException;
import blockchains.iaas.uni.stuttgart.de.api.model.Parameter;
import jakarta.xml.bind.DatatypeConverter;
import org.web3j.abi.datatypes.*;

import java.math.BigInteger;

public class ParameterEncoder {
    public static Type encode(Parameter parameter) throws ParameterException {
        Class<? extends Type> typeClass = EthereumTypeMapper.getEthereumType(parameter.getType());
        try {
            if (typeClass == Bool.class) {
                return new Bool(Boolean.parseBoolean(parameter.getValue()));
            }

            if (typeClass == Address.class) {
                return new Address(parameter.getValue());
            }

            if (typeClass == Utf8String.class) {
                return new Utf8String(parameter.getValue());
            }

            if (typeClass == DynamicBytes.class) {
                return new DynamicBytes(DatatypeConverter.parseHexBinary(parameter.getValue()));
            }

            if (typeClass.getSuperclass() == Int.class) {
                return typeClass.getDeclaredConstructor(BigInteger.class)
                        .newInstance(new BigInteger(parameter.getValue(), 10));
            }

            if (typeClass.getSuperclass() == Uint.class) {
                return typeClass.getDeclaredConstructor(BigInteger.class)
                        .newInstance(new BigInteger(parameter.getValue(), 10));
            }

            if (typeClass.getSuperclass() == Bytes.class) {
                byte[] value = DatatypeConverter.parseHexBinary(parameter.getValue());
                return typeClass.getDeclaredConstructor(value.getClass())
                        .newInstance((Object) value);
            }

            throw new ParameterException("Unrecognized parameter type!");
        } catch (Exception e) {
            if (e instanceof ParameterException)
                throw (ParameterException) e;

            throw new ParameterException(e.getMessage());
        }
    }
}
