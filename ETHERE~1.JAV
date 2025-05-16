/*******************************************************************************
 * Copyright (c) 2019-2024 Institute for the Architecture of Application System - University of Stuttgart
 * Author: Ghareeb Falazi
 * Co-author: Akshay Patel
 *
 * This program and the accompanying materials are made available under the
 * terms the Apache Software License 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: Apache-2.0
 *******************************************************************************/
package blockchains.iaas.uni.stuttgart.de.plugin.ethereum;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.naming.OperationNotSupportedException;


import blockchains.iaas.uni.stuttgart.de.api.exceptions.*;
import blockchains.iaas.uni.stuttgart.de.api.interfaces.BlockchainAdapter;
import blockchains.iaas.uni.stuttgart.de.api.interfaces.FinalityConfidenceCalculator;
import blockchains.iaas.uni.stuttgart.de.api.model.*;

import blockchains.iaas.uni.stuttgart.de.api.utils.BooleanExpressionEvaluator;
import blockchains.iaas.uni.stuttgart.de.api.utils.PoWConfidenceCalculator;
import blockchains.iaas.uni.stuttgart.de.api.utils.SmartContractPathParser;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import okhttp3.OkHttpClient;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.EventValues;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.WalletUtils;
import org.web3j.crypto.exception.CipherException;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.JsonRpc2_0Web3j;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthGetTransactionCount;
import org.web3j.protocol.core.methods.response.EthGetTransactionReceipt;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.EthTransaction;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.protocol.http.HttpService;
import org.web3j.tx.Contract;
import org.web3j.tx.Transfer;
import org.web3j.tx.gas.DefaultGasProvider;
import org.web3j.utils.Async;
import org.web3j.utils.Convert;

@Log4j2
public class EthereumAdapter implements BlockchainAdapter {
    private Credentials credentials;
    private final String nodeUrl;
    @Getter
    private final Web3j web3j;
    private final DateTimeFormatter formatter;
    private final int averageBlockTimeSeconds;
    private final String resourceManagerSmartContractAddress;
    @Setter
    @Getter
    protected FinalityConfidenceCalculator confidenceCalculator;

    public EthereumAdapter(final String nodeUrl, final int averageBlockTimeSeconds, String resourceManagerSmartContractAddress) {
        this.nodeUrl = nodeUrl;
        this.averageBlockTimeSeconds = averageBlockTimeSeconds;
        // We use a specific implementation, so we can change the polling period (useful for prototypes).
        this.web3j = new JsonRpc2_0Web3j(createWeb3HttpService(this.nodeUrl), this.averageBlockTimeSeconds, Async.defaultExecutorService());
        this.formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        this.resourceManagerSmartContractAddress = resourceManagerSmartContractAddress;
    }


    Credentials getCredentials() {
        return credentials;
    }

    void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    public void setCredentials(String password, String fileSource) throws IOException, CipherException {
        try {
            this.credentials = WalletUtils.loadCredentials(password, fileSource);
        } catch (IOException | CipherException e) {
            log.error("Error occurred while setting the user credentials for Ethereum. Reason {}", e.getMessage());
            throw e;
        }
    }

    String testConnectionToNode() {
        try {
            log.info("Connected to Ethereum client: URL: {}, Version: {}", this.nodeUrl, this.web3j.web3ClientVersion().send().getWeb3ClientVersion());
            return "true";
        } catch (IOException e) {
            log.error("Failed to connect to Ethereum client at URL: {}. Reason: {}", this.nodeUrl, e.getMessage());

            return e.getMessage();
        }
    }

    /**
     * Subscribes for the event of detecting a transition of the state of a given transaction, which is assumed to having been
     * MINED before. The method supports detecting:
     * (i) a transaction being not found anymore (invalidated): NOT_FOUND,
     * (ii) not having a containing block (orphaned): PENDING: ,
     * (iii) reporting an error although mined into a block (e.g., SC function threw an error): ERRORED
     * (iii) having received enough block-confirmations (durably committed): CONFIRMED.
     *
     * @param txHash         the hash of the transaction to monitor
     * @param waitFor        the number of block-confirmations to wait until the transaction is considered persisted (-1 if the
     *                       transaction is never to be considered persisted)
     * @param observedStates the set of states that will be reported to the calling method
     * @return a future which is used to handle the subscription and receive the callback
     */
    private CompletableFuture<Transaction> subscribeForTxEvent(String txHash, long waitFor, TransactionState... observedStates) {
        final CompletableFuture<Transaction> result = new CompletableFuture<>();
        final Disposable subscription = web3j.blockFlowable(false).subscribe(ethBlock -> {
            try {
                // make sure the transaction exists
                final EthTransaction transaction = web3j.ethGetTransactionByHash(txHash).send();
                // if not, then it is either invalidated or did not exist in the first place
                if (transaction.getTransaction().isEmpty()) {
                    final String msg = String.format("The transaction of the hash %s is not found!", txHash);
                    log.info(msg);
                    handleDetectedState(transaction.getTransaction(), TransactionState.NOT_FOUND, observedStates, result);

                    return;
                }

                // determine if the transaction reported an error
                EthGetTransactionReceipt receipt = web3j.ethGetTransactionReceipt(txHash).send();

                if (!receipt.getResult().isStatusOK()) {
                    if (handleDetectedState(transaction.getTransaction(), TransactionState.ERRORED, observedStates, result))
                        return;
                }

                // make sure the transaction is still contained in a block, i.e., it was not orphaned
                final String retrievedBlockHash = transaction.getTransaction().get().getBlockHash();

                if (retrievedBlockHash == null || retrievedBlockHash.isEmpty()) {
                    final String msg = String.format("The transaction of the hash %s has no block (orphaned?)", txHash);
                    log.info(msg);

                    handleDetectedState(transaction.getTransaction(), TransactionState.PENDING, observedStates, result);
                    return;
                }
                // check if enough block-confirmations have occurred.
                if (waitFor >= 0 && ethBlock.getBlock() != null) {
                    if (ethBlock.getBlock().getNumber()
                            .subtract(transaction.getTransaction().get().getBlockNumber())
                            .intValue() >= waitFor) {
                        final String msg = String.format("The transaction of the hash %s has been confirmed",
                                txHash);
                        log.info(msg);

                        handleDetectedState(transaction.getTransaction(), TransactionState.CONFIRMED, observedStates, result);
                    }
                }
            } catch (IOException e) {
                result.completeExceptionally(e);
            }
        });

        //dispose the flowable when the CompletableFuture completes (either when detecting an event, or manually)
        result.whenComplete((v, e) -> subscription.dispose());

        return result;
    }

    private static CompletionException wrapEthereumExceptions(Throwable e) {
        return new CompletionException(mapEthereumException(e));
    }

    private static BalException mapEthereumException(Throwable e) {
        BalException result;

        if (e instanceof BalException)
            result = (BalException) e;
        else if (e.getCause() instanceof BalException)
            result = (BalException) e.getCause();
        else if (e.getCause() instanceof IOException)
            result = new BlockchainNodeUnreachableException(e.getMessage());
        else if (e instanceof IllegalArgumentException || e instanceof OperationNotSupportedException)
            result = new InvokeSmartContractFunctionFailure(e.getMessage());
        else if (e.getCause() instanceof RuntimeException)
            result = new InvalidTransactionException(e.getMessage());
        else {
            log.error("Unexpected exception was thrown!");
            result = new InvalidTransactionException(e.getMessage());
        }

        return result;
    }

    @Override
    public CompletableFuture<Transaction> submitTransaction(String receiverAddress, BigDecimal value, double requiredConfidence)
            throws InvalidTransactionException {
        if (credentials == null) {
            log.error("Credentials are not set for the Ethereum user");
            throw new NullPointerException("Credentials are not set for the Ethereum user");
        }

        try {
            final long waitFor = ((PoWConfidenceCalculator) this.confidenceCalculator).getEquivalentBlockDepth(requiredConfidence);
            return Transfer.sendFunds(web3j, credentials, receiverAddress, value, Convert.Unit.WEI)  // 1 wei = 10^-18 Ether
                    .sendAsync()
                    // when an exception (e.g., ConnectException happens), the following is skipped
                    .thenCompose(tx -> {
                        if (!tx.isStatusOK()) {
                            // we go here, for example, for block gas limit exceptions.
                            InvalidTransactionException exception = new InvalidTransactionException(
                                    "Failed to submit transaction. Ethereum JSON-RPC code: " + tx.getStatus());
                            log.atError().withThrowable(exception).log("Failed to submit transaction. Ethereum JSON-RPC code: {}", () -> tx.getStatus());
                            throw new CompletionException(exception);
                        }

                        return subscribeForTxEvent(tx.getTransactionHash(), waitFor, TransactionState.CONFIRMED, TransactionState.NOT_FOUND);
                    })
                    .exceptionally((e) -> {
                                throw wrapEthereumExceptions(e);
                            }
                    );
        } catch (Exception e) {// this seems to never get invoked
            final String msg = "An error occurred while trying to submit a new transaction to ethereum. Reason: " + e.getMessage();
            log.error(msg);

            throw new InvalidTransactionException(msg);
        }
    }

    @Override
    public Observable<Transaction> receiveTransactions(String senderId, double requiredConfidence) {
        if (credentials == null) {
            log.error("Credentials are not set for the Ethereum user");
            throw new NullPointerException("Credentials are not set for the Ethereum user");
        }

        final long waitFor = ((PoWConfidenceCalculator) this.confidenceCalculator).getEquivalentBlockDepth(requiredConfidence);
        final String myAddress = credentials.getAddress();
        final PublishSubject<Transaction> result = PublishSubject.create();
        final Disposable newTransactionObservable = web3j.transactionFlowable().subscribe(tx -> {
            if (myAddress.equalsIgnoreCase(tx.getTo())) {
                if (senderId == null || senderId.trim().isEmpty() || senderId.equalsIgnoreCase(tx.getFrom())) {
                    log.info("New transaction received from:" + tx.getFrom());
                    subscribeForTxEvent(tx.getHash(), waitFor, TransactionState.CONFIRMED)
                            .thenAccept(result::onNext)
                            .exceptionally(error -> {
                                result.onError(wrapEthereumExceptions(error));
                                return null;
                            });
                }
            }
        }, e -> result.onError(wrapEthereumExceptions(e)));

        return result.doFinally(newTransactionObservable::dispose);
    }

    @Override
    public CompletableFuture<TransactionState> ensureTransactionState(String transactionId, double requiredConfidence) {
        final long waitFor = ((PoWConfidenceCalculator) this.confidenceCalculator).getEquivalentBlockDepth(requiredConfidence);
        // only monitor the transition into the CONFIRMED state or the NOT_FOUND state
        return subscribeForTxEvent(transactionId, waitFor, TransactionState.CONFIRMED, TransactionState.NOT_FOUND)
                .thenApply(Transaction::getState)
                .exceptionally((e) -> {
                    throw wrapEthereumExceptions(e);
                });
    }

    @Override
    public CompletableFuture<TransactionState> detectOrphanedTransaction(String transactionId) {
        // only monitor the transition into the PENDING state
        return subscribeForTxEvent(transactionId, -1, TransactionState.PENDING, TransactionState.NOT_FOUND)
                .thenApply(Transaction::getState)
                .exceptionally((e) -> {
                    throw wrapEthereumExceptions(e);
                });
    }

    @Override
    public CompletableFuture<Transaction> invokeSmartContract(
            String smartContractPath,
            String functionIdentifier,
            List<Parameter> inputs,
            List<Parameter> outputs,
            double requiredConfidence,
            long timeoutMillis,
            boolean sideEffects
    ) throws NotSupportedException, ParameterException {
        if (credentials == null) {
            log.error("Credentials are not set for the Ethereum user");
            throw new NullPointerException("Credentials are not set for the Ethereum user");
        }

        Objects.requireNonNull(smartContractPath);
        Objects.requireNonNull(functionIdentifier);
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(outputs);

        try {
            long waitFor = ((PoWConfidenceCalculator) this.confidenceCalculator).getEquivalentBlockDepth(requiredConfidence);
            final String[] pathSegments = SmartContractPathParser.parse(smartContractPath).getSmartContractPathSegments();

            if (pathSegments.length != 1) {
                throw new SmartContractNotFoundException("Malformed Ethereum path!");
            }

            if (!pathSegments[0].matches("^0x[a-fA-F0-9]{40}$")) {
                throw new SmartContractNotFoundException("Malformed Ethereum address!");
            }

            final String smartContractAddress = pathSegments[0];
            List<TypeReference<?>> outputParameters = new ArrayList<>();
            Class<? extends Type> currentReturnType;

            for (Parameter output : outputs) {
                currentReturnType = EthereumTypeMapper.getEthereumType(output.getType());
                outputParameters.add(TypeReference.create(currentReturnType));
            }

            final Function function = new Function(
                    functionIdentifier,  // function we're calling
                    this.convertToSolidityTypes(inputs),  // Parameters to pass as Solidity Types
                    outputParameters); //Type of returned value

            final String encodedFunction = FunctionEncoder.encode(function);

            // if there are no side effects, we invoke as a method call, otherwise, we try a transaction
            if (!sideEffects) {
                return this.invokeFunctionByMethodCall(
                        encodedFunction,
                        smartContractAddress,
                        outputs,
                        function.getOutputParameters());
            } else {
                return this.invokeFunctionByTransaction(
                        waitFor,
                        encodedFunction,
                        smartContractAddress,
                        timeoutMillis);
            }
        } catch (Exception e) {
            log.error("Decoding smart contract function call failed. Reason: {}", e.getMessage());
            throw mapEthereumException(e);
        }
    }

    @Override
    public Observable<Occurrence> subscribeToEvent(String smartContractAddress, String eventIdentifier,
                                                   List<Parameter> outputParameters, double degreeOfConfidence, String filter) throws BalException {
        long waitFor = ((PoWConfidenceCalculator) this.confidenceCalculator).getEquivalentBlockDepth(degreeOfConfidence);
        List<TypeReference<?>> types = this.convertTypes(outputParameters);
        final Event event = new Event(eventIdentifier, types);
        final EthFilter ethFilter = this.generateSubscriptionFilter(smartContractAddress, event);
        final PublishSubject<Occurrence> result = PublishSubject.create();

        Disposable newEventObservable = web3j.ethLogFlowable(ethFilter).subscribe(log -> {
            Occurrence occurrence = this.handleLog(log, event, outputParameters, filter);

            // if the result is null, then the filter has evaluated to false.
            if (occurrence != null) {
                this.subscribeForTxEvent(log.getTransactionHash(), waitFor, TransactionState.CONFIRMED)
                        .thenAccept(tx -> result.onNext(occurrence))
                        .exceptionally(error -> {
                            result.onError(wrapEthereumExceptions(error));
                            return null;
                        });
            }
        }, e -> result.onError(wrapEthereumExceptions(e)));

        return result.doFinally(newEventObservable::dispose);
    }

    @Override
    public CompletableFuture<QueryResult> queryEvents(String smartContractAddress, String eventIdentifier,
                                                      List<Parameter> outputParameters, String filter, TimeFrame timeFrame) throws BalException {
        List<TypeReference<?>> types = this.convertTypes(outputParameters);
        final Event event = new Event(eventIdentifier, types);
        try {
            final EthFilter ethFilter = this.generateQueryFilter(smartContractAddress, event, timeFrame);
            return web3j.ethGetLogs(ethFilter)
                    .sendAsync()
                    .thenApply(result -> {
                        try {
                            List<Occurrence> finalResult = new ArrayList<>();

                            for (EthLog.LogResult logResult : result.getLogs()) {
                                Log log = (Log) logResult.get();
                                Occurrence occurrence = this.handleLog(log, event, outputParameters, filter);

                                if (occurrence != null) {
                                    finalResult.add(occurrence);
                                }
                            }

                            return QueryResult.builder().occurrences(finalResult).build();
                        } catch (Exception e) {
                            throw new CompletionException(new InvalidScipParameterException("The filter script is invalid: " + e.getMessage()));
                        }
                    });
        } catch (IOException e) {
            throw new BlockchainNodeUnreachableException(e.getMessage());
        }
    }

    @Override
    public ResourceManagerSmartContract getResourceManagerSmartContract() throws NotSupportedException {
        Parameter txId = new Parameter("txId",
                "{ \"Name\": \"txId\", \"Type\": \"string\" }",
                null);
        List<Parameter> txIdAsList = new ArrayList<>();
        List<Parameter> emptyList = new ArrayList<>();
        txIdAsList.add(txId);
        SmartContractFunction prepare = new SmartContractFunction("prepare", txIdAsList, emptyList);
        SmartContractFunction commit = new SmartContractFunction("commit", txIdAsList, emptyList);
        SmartContractFunction abort = new SmartContractFunction("abort", txIdAsList, emptyList);
        Parameter owner = new Parameter("owner",
                "{ \"Name\": \"owner\", \"Type\": \"address\" }",
                null);
        Parameter isYes = new Parameter("isYes",
                "{ \"Name\": \"isYes\", \"Type\": \"bool\" }",
                null);
        List<Parameter> votedEventParams = new ArrayList<>();
        votedEventParams.add(owner);
        votedEventParams.add(txId);
        votedEventParams.add(isYes);
        List<Parameter> abortedEventParams = new ArrayList<>();
        abortedEventParams.add(owner);
        abortedEventParams.add(txId);
        SmartContractEvent voted = new SmartContractEvent("Voted", votedEventParams);
        SmartContractEvent aborted = new SmartContractEvent("TxAborted", abortedEventParams);
        List<SmartContractFunction> functions = new ArrayList<>();
        functions.add(prepare);
        functions.add(commit);
        functions.add(abort);
        List<SmartContractEvent> events = new ArrayList<>();
        events.add(voted);
        events.add(aborted);

        return new EthereumResourceManagerSmartContract(this.resourceManagerSmartContractAddress, functions, events);
    }

    @Override
    public String testConnection() {
        return this.testConnectionToNode();
    }

    private List<TypeReference<?>> convertTypes(List<Parameter> parameters) {
        return parameters
                .stream()
                .map(param -> (EthereumTypeMapper.getEthereumType(param.getType())))
                .map(TypeReference::create)
                .collect(Collectors.toList());
    }

    private Occurrence handleLog(Log log, Event event, List<Parameter> outputParameters, String filter) throws Exception {
        final EventValues values = Contract.staticExtractEventParameters(event, log);
        List<Parameter> parameters = new ArrayList<>();

        for (int i = 0; i < outputParameters.size(); i++) {
            parameters.add(Parameter.builder()
                    .name(outputParameters.get(i).getName())
                    .type(outputParameters.get(i).getType())
                    .value(ParameterDecoder.decode(values.getNonIndexedValues().get(i)))
                    .build());
        }

        if (BooleanExpressionEvaluator.evaluate(filter, parameters)) {
            EthBlock block = this.web3j.ethGetBlockByHash(log.getBlockHash(), false).send();
            LocalDateTime timestamp = LocalDateTime.ofEpochSecond(block.getBlock().getTimestamp().longValue(), 0, ZoneOffset.UTC);
            String timestampS = formatter.format(timestamp);

            return Occurrence.builder().parameters(parameters).isoTimestamp(timestampS).build();
        }

        return null;
    }

    private EthFilter generateSubscriptionFilter(String smartContractAddress, Event event) {
        return this.generateFilter(smartContractAddress, event, DefaultBlockParameterName.LATEST, DefaultBlockParameterName.LATEST);
    }

    private EthFilter generateQueryFilter(String smartContractAddress, Event event, TimeFrame timeFrame) throws IOException {
        DefaultBlockParameter from;
        DefaultBlockParameter to;

        if (timeFrame == null) {
            from = DefaultBlockParameterName.EARLIEST;
            to = DefaultBlockParameterName.LATEST;
        } else {

            if (timeFrame.getFrom() == null || timeFrame.getTo().isEmpty()) {
                from = DefaultBlockParameterName.EARLIEST;
            } else {
                LocalDateTime fromDateTime = LocalDateTime.parse(timeFrame.getFrom(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                long fromBlockNumber = this.getBlockAfterIsoDate(fromDateTime);
                from = new DefaultBlockParameterNumber(fromBlockNumber);
            }

            if (timeFrame.getTo() == null || timeFrame.getFrom().isEmpty()) {
                to = DefaultBlockParameterName.LATEST;
            } else {
                LocalDateTime toDateTime = LocalDateTime.parse(timeFrame.getTo(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                long toBlockNumber = this.getBlockAfterIsoDate(toDateTime);

                // this indicates the specified date is after the last block
                if (toBlockNumber == Long.MAX_VALUE) {
                    to = DefaultBlockParameterName.LATEST;
                } else {
                    if (toBlockNumber > 0)
                        toBlockNumber--;
                    else
                        throw new InvalidScipParameterException();

                    to = new DefaultBlockParameterNumber(toBlockNumber);
                }
            }
        }

        return this.generateFilter(smartContractAddress, event, from, to);
    }

    long getBlockAfterIsoDate(final LocalDateTime dateTime) throws IOException {
        final long seconds = Duration.between(dateTime, LocalDateTime.now()).getSeconds();
        long estimatedBlockLag = seconds / averageBlockTimeSeconds;

        // if the block is in the future
        if (estimatedBlockLag < 0) {
            estimatedBlockLag = 0;
        }

        final long latestBlockNumber = web3j.ethBlockNumber().send().getBlockNumber().longValue();
        long blockNumber = latestBlockNumber - estimatedBlockLag;

        // if the estimated block is before the genesis
        if (blockNumber < 0) {
            blockNumber = 0;
        }

        BigInteger blockTimeStamp = web3j.ethGetBlockByNumber(new DefaultBlockParameterNumber(blockNumber), false).send().getBlock().getTimestamp();
        LocalDateTime blockDateTime = LocalDateTime.ofEpochSecond(blockTimeStamp.longValue(), 0, ZoneOffset.UTC);

        // decide on direction
        if (blockDateTime.isAfter(dateTime)) {
            while (--blockNumber >= 0) {
                blockTimeStamp = web3j.ethGetBlockByNumber(new DefaultBlockParameterNumber(blockNumber), false).send().getBlock().getTimestamp();
                blockDateTime = LocalDateTime.ofEpochSecond(blockTimeStamp.longValue(), 0, ZoneOffset.UTC);

                if (blockDateTime.isBefore(dateTime)) {
                    return blockNumber + 1;
                }
            }

            return 0;
        }

        while (++blockNumber <= latestBlockNumber) {
            blockTimeStamp = web3j.ethGetBlockByNumber(new DefaultBlockParameterNumber(blockNumber), false).send().getBlock().getTimestamp();
            blockDateTime = LocalDateTime.ofEpochSecond(blockTimeStamp.longValue(), 0, ZoneOffset.UTC);

            if (blockDateTime.isAfter(dateTime)) {
                return blockNumber;
            }
        }

        return Long.MAX_VALUE;
    }

    private EthFilter generateFilter(String smartContractAddress, Event event, DefaultBlockParameter from, DefaultBlockParameter to) {

        return new EthFilter(
                from,
                to,
                smartContractAddress).
                addSingleTopic(EventEncoder.encode(event));
    }

    private CompletableFuture<Transaction> invokeFunctionByMethodCall(String encodedFunction, String scAddress, List<Parameter> outputs,
                                                                      List<TypeReference<Type>> returnTypes) {
        org.web3j.protocol.core.methods.request.Transaction transaction = org.web3j.protocol.core.methods.request.Transaction
                .createEthCallTransaction(credentials.getAddress(), scAddress, encodedFunction);

        return web3j.ethCall(transaction, DefaultBlockParameterName.LATEST)
                .sendAsync()
                .thenApply(ethCall -> FunctionReturnDecoder.decode(ethCall.getValue(), returnTypes))
                .thenApply(decoded -> {
                    if (returnTypes.size() != decoded.size())
                        throw new InvokeSmartContractFunctionFailure("Failed to invoke read-only Ethereum smart contract function");

                    Transaction tx = new LinearChainTransaction();
                    tx.setState(TransactionState.RETURN_VALUE);
                    List<Parameter> returnedValues = new ArrayList<>();

                    for (int i = 0; i < decoded.size(); i++) {
                        returnedValues.add(Parameter
                                .builder()
                                .name(outputs.get(i).getName())
                                .value(ParameterDecoder.decode(decoded.get(i)))
                                .build());
                    }

                    tx.setReturnValues(returnedValues);

                    return tx;
                });
    }

    private CompletableFuture<Transaction> invokeFunctionByTransaction(long waitFor, String encodedFunction, String scAddress, long timeoutMillis) {
        return this
                .retrieveNewNonce()
                .thenApply(nonce -> {
                    try {
                        return org.web3j.protocol.core.methods.request.Transaction.createFunctionCallTransaction(
                                credentials.getAddress(),
                                nonce,
                                DefaultGasProvider.GAS_PRICE,
                                DefaultGasProvider.GAS_LIMIT,
                                scAddress,
                                encodedFunction);
                    } catch (Exception e) {
                        log.error("An error occurred while trying to create a function signature!. Reason: {}", e.getMessage());
                        throw new CompletionException(wrapEthereumExceptions(e));
                    }
                })
                .thenCompose(transaction -> web3j.ethSendTransaction(transaction).sendAsync())
                .thenCompose(tx -> {
                    final String txHash = tx.getTransactionHash();

                    if (tx.hasError()) {
                        InvokeSmartContractFunctionFailure exception = new InvokeSmartContractFunctionFailure(
                                "Failed to invoke smart contract function via tx. Reason: " + tx.getError().getMessage());
                        log.error("Transaction submission failed. Code: {}", tx.getError().getCode(), exception);
                        throw new CompletionException(exception);
                    }

                    log.info("transaction hash is {}", txHash);
                    return CompletableFuture.completedFuture(txHash);
                })
                .thenCompose(txHash -> waitUntilTransactionIsMined(txHash, timeoutMillis))
                .thenCompose(txReceipt -> subscribeForTxEvent(txReceipt.getTransactionHash(), waitFor,
                        TransactionState.CONFIRMED, TransactionState.NOT_FOUND, TransactionState.ERRORED))
                .exceptionally((e) -> {
                    throw wrapEthereumExceptions(e);
                });
    }

    private CompletableFuture<TransactionReceipt> waitUntilTransactionIsMined(final String txHash, final long timeOutMillis)
            throws CompletionException {
        log.info("Started waiting until transaction with hash {} is mined. Timeout: {}", txHash, timeOutMillis);
        final CompletableFuture<TransactionReceipt> result = new CompletableFuture<>();
        final long START_TIME_MILLIS = (new Date()).getTime();

        final Disposable subscription = web3j.blockFlowable(false).subscribe(ethBlock -> {
            try {
                long currentTimeMillis = (new Date()).getTime();

                // if the time passed since we started is longer than the timeout
                if (currentTimeMillis - START_TIME_MILLIS >= timeOutMillis) {
                    TimeoutException exception =
                            new TimeoutException("Timeout is reached before transaction is mined!", txHash, 0.0);

                    result.completeExceptionally(exception);
                } else {
                    EthGetTransactionReceipt receipt = web3j.ethGetTransactionReceipt(txHash).send();

                    if (receipt != null && receipt.getTransactionReceipt().isPresent()) {
                        log.info("Successfully waited until transaction is mined. Result: {}", () -> receipt.getResult());
                        result.complete(receipt.getResult());
                    }
                }
            } catch (IOException e) {
                log.error("Failed to wait until transaction is mined.", e);
                result.completeExceptionally(e);
            }
        }, error -> {
            log.error("Failed to wait until transaction is mined.", error);
            result.completeExceptionally(error);
        });

        //dispose the flowable when the CompletableFuture completes (either when detecting an event, or manually)
        result.whenComplete((v, e) -> subscription.dispose());

        return result;
    }

    // based on https://github.com/web3j/web3j/blob/master/abi/src/test/java/org/web3j/abi/FunctionEncoderTest.java
    private List<Type> convertToSolidityTypes(List<Parameter> params) throws ParameterException {
        List<Type> result = new ArrayList<>();

        for (Parameter param : params) {
            result.add(ParameterEncoder.encode(param));
        }

        return result;
    }

    private CompletableFuture<BigInteger> retrieveNewNonce() {
        return web3j.ethGetTransactionCount(
                        credentials.getAddress(), DefaultBlockParameterName.LATEST)
                .sendAsync()
                .thenApply(EthGetTransactionCount::getTransactionCount);
    }

    private static boolean handleDetectedState(final Optional<org.web3j.protocol.core.methods.response.Transaction> transactionDetails,
                                               final TransactionState detectedState, final TransactionState[] interesting,
                                               CompletableFuture<Transaction> future) {
        // Only complete the future if we are interested in this event
        if (Arrays.asList(interesting).contains(detectedState)) {
            final LinearChainTransaction result = new LinearChainTransaction();
            result.setState(detectedState);
            // it is important that this list is not null
            result.setReturnValues(new ArrayList<>());

            if (transactionDetails.isPresent()) {
                result.setBlock(new Block(transactionDetails.get().getBlockNumber(), transactionDetails.get().getBlockHash()));
                result.setFrom(transactionDetails.get().getFrom());
                result.setTo(transactionDetails.get().getTo());
                result.setTransactionHash(transactionDetails.get().getHash());
                result.setValue(transactionDetails.get().getValue());
            }

            future.complete(result);

            return true;
        }

        return false;
    }

    private static HttpService createWeb3HttpService(String url) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OkHttpClient client = builder
                .connectTimeout(0, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .writeTimeout(0, TimeUnit.SECONDS)
                .build();
        return new HttpService(url, client, false);
    }
}
