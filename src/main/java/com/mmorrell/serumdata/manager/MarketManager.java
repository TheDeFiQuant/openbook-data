package com.mmorrell.serumdata.manager;

import ch.openserum.serum.model.Market;
import ch.openserum.serum.model.MarketBuilder;
import ch.openserum.serum.model.SerumUtils;
import com.mmorrell.serumdata.util.MarketUtil;
import com.mmorrell.serumdata.util.RpcUtil;
import org.p2p.solanaj.core.PublicKey;
import org.p2p.solanaj.rpc.RpcClient;
import org.p2p.solanaj.rpc.RpcException;
import org.p2p.solanaj.rpc.types.ConfirmedTransaction;
import org.p2p.solanaj.rpc.types.Memcmp;
import org.p2p.solanaj.rpc.types.ProgramAccount;
import org.p2p.solanaj.rpc.types.SignatureInformation;
import org.p2p.solanaj.rpc.types.config.Commitment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class MarketManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketManager.class);
    private static final int MARKET_CACHE_TIMEOUT_SECONDS = 40;
    // <tokenMint, List<Market>>
    private final Map<String, List<Market>> marketMapCache = new HashMap<>();
    // <marketId, Builder>
    private final Map<String, MarketBuilder> marketBuilderCache = new HashMap<>();
    private final RpcClient client = new RpcClient(RpcUtil.getPublicEndpoint(), MARKET_CACHE_TIMEOUT_SECONDS);
    private final Map<String, CompletableFuture<Void>> tradeHistoryKeyToFutureMap = new HashMap<>();

    // <concat(marketId, ooa, owner), jupiterTx>
    private final Map<String, Optional<String>> jupiterTxMap = new HashMap<>();

    // Jupiter
    private static final PublicKey JUPITER_PROGRAM_ID =
            PublicKey.valueOf("JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph");
    private static final PublicKey JUPITER_USDC_WALLET =
            PublicKey.valueOf("H5sizxhR6ssXrX2YNDoYaUv93PU34VzyRaVaUHuo5eFk");
    private static final PublicKey JUPITER_USDT_WALLET =
            PublicKey.valueOf("FVKG6bkrQ4rksme6GT1FN7PgvZf9cNmupyWfN5kJj8Fx");
    private static final PublicKey JUPITER_WSOL_WALLET =
            PublicKey.valueOf("61CjGbapEVoyCC51x5tPZGZHCYsgtPSSssCatHEEUWeG");

    // Cache USDC and SOL quoted markets.
    public final Set<PublicKey> quoteMintsToCache = Set.of(
            MarketUtil.USDC_MINT,
            MarketUtil.USDT_MINT,
            SerumUtils.WRAPPED_SOL_MINT,
            PublicKey.valueOf("mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"), // MSOL
            PublicKey.valueOf("A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM"), // USDCet
            PublicKey.valueOf("7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj"), // stSOL
            PublicKey.valueOf("4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"), // RAY
            PublicKey.valueOf("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"), // ETH (Portal)
            PublicKey.valueOf("7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT"), // UXD
            PublicKey.valueOf("2FPyTwcZLUg1MDrwsyoP4D6s1tM7hAkHYRjkNb5w6Pxk"), // soETH
            PublicKey.valueOf("9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"), // BTC (Sollet)
            PublicKey.valueOf("SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"), // SRM
            PublicKey.valueOf("BQcdHdAQW1hczDbBi9hiegXAR7A98Q9jx3X3iBBBDiq4"), // soUSDT
            PublicKey.valueOf("ATLASXmbPQxBUYbxPsV97usA3fPQYEqzQBUHgiFCUsXx") // ATLAS
    );

    public MarketManager() {
        updateMarkets();
    }

    public Map<String, List<Market>> getMarketMapCache() {
        return marketMapCache;
    }

    public List<Market> getMarketCache() {
        return marketMapCache.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public List<Market> getMarketsByMint(String tokenMint) {
        return marketMapCache.getOrDefault(tokenMint, new ArrayList<>());
    }

    public void addBuilderToCache(MarketBuilder marketBuilder) {
        marketBuilderCache.put(marketBuilder.getPublicKey().toBase58(), marketBuilder);
    }

    public boolean isBuilderCached(String marketId) {
        return marketBuilderCache.containsKey(marketId);
    }

    public MarketBuilder getBuilderFromCache(String marketId) {
        return marketBuilderCache.get(marketId);
    }

    /**
     * Update marketCache with the latest markets
     */
    public void updateMarkets() {
        LOGGER.info(
                String.format(
                        "Caching markets for quoteMints: %s",
                        quoteMintsToCache.stream().map(PublicKey::toBase58).collect(Collectors.joining(", "))
                )
        );

        marketMapCache.clear();
        final Collection<ProgramAccount> programAccounts = new ConcurrentLinkedQueue<>();
        final List<CompletableFuture<Void>> marketCacheThreads = new ArrayList<>();

        for (PublicKey quoteMint : quoteMintsToCache) {
            // Create each thread
            final CompletableFuture<Void> marketCacheThread = CompletableFuture.supplyAsync(() -> {
                LOGGER.info("Caching (w/ random delay): " + quoteMint.toBase58());
                int delay = new Random().nextInt(12000);

                // Random delay to not get rate limited.
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                LOGGER.info("Random delay complete, requesting: " + quoteMint.toBase58());
                try {
                    programAccounts.addAll(
                            client.getApi().getProgramAccounts(
                                    SerumUtils.SERUM_PROGRAM_ID_V3,
                                    List.of(
                                            new Memcmp(
                                                    SerumUtils.QUOTE_MINT_OFFSET,
                                                    quoteMint.toBase58()
                                            )
                                    ),
                                    SerumUtils.MARKET_ACCOUNT_SIZE
                            )
                    );
                    LOGGER.info("Cached: " + quoteMint.toBase58());
                } catch (RpcException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            marketCacheThreads.add(marketCacheThread);
        }

        final CompletableFuture<Void> combinedFutures =
                CompletableFuture.allOf(marketCacheThreads.toArray(new CompletableFuture[0]));
        try {
            // Wait for all threads to complete.
            combinedFutures.get();
            LOGGER.info("Market caching threads complete.");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        for (ProgramAccount programAccount : programAccounts) {
            Market market = Market.readMarket(programAccount.getAccount().getDecodedData());

            // Get list of existing markets for this base mint. otherwise create a new list and put it there.
            List<Market> existingMarketList = marketMapCache.getOrDefault(market.getBaseMint().toBase58(), new ArrayList<>());
            existingMarketList.add(market);

            if (existingMarketList.size() == 1) {
                marketMapCache.put(market.getBaseMint().toBase58(), existingMarketList);
            }
        }

        LOGGER.info(
                String.format(
                        "Markets cached for quoteMints: %s",
                        quoteMintsToCache.stream().map(PublicKey::toBase58).collect(Collectors.joining(", "))
                )
        );
    }

    public int numMarketsByToken(String tokenMint) {
        return marketMapCache.getOrDefault(tokenMint, new ArrayList<>()).size();
    }

    public Optional<Market> getMarketById(String marketId) {
        return marketMapCache.values().stream()
                .flatMap(Collection::stream)
                .filter(market -> market.getOwnAddress().toBase58().equals(marketId))
                .findAny();
    }

    public Optional<String> getJupiterTxForMarketAndOoa(String marketId, String ooa, String owner, float price, float quantity) {
        String uniqueKey = marketId.concat(ooa).concat(owner).concat(String.valueOf(price)).concat(String.valueOf(quantity));
        // bail out if its cached
        if (jupiterTxMap.containsKey(uniqueKey)) {
            // LOGGER.info("HAVE ANSWER: " + uniqueKey);
            return jupiterTxMap.get(uniqueKey);
        }

        // bail if were already working on it
        if (tradeHistoryKeyToFutureMap.containsKey(uniqueKey)) {
            if (!tradeHistoryKeyToFutureMap.get(uniqueKey).isDone()) {
                // LOGGER.info("WORKING: " + uniqueKey);
                return Optional.empty();
            }
        }


        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            List<SignatureInformation> confirmedSignatures;
            try {
                confirmedSignatures = client.getApi().getSignaturesForAddress(
                        PublicKey.valueOf(owner),
                        10,
                        Commitment.CONFIRMED
                );
            } catch (RpcException e) {
                throw new RuntimeException(e);
            }

            for (SignatureInformation signatureInformation : confirmedSignatures) {
                ConfirmedTransaction confirmedTransaction;
                try {
                    confirmedTransaction = client.getApi().getTransaction(signatureInformation.getSignature(), Commitment.CONFIRMED);
                } catch (RpcException e) {
                    throw new RuntimeException(e);
                }
                if (confirmedTransaction.getTransaction() == null || confirmedTransaction.getTransaction().getMessage() == null) {
                    break;
                }
                for (ConfirmedTransaction.Instruction instruction : confirmedTransaction.getTransaction().getMessage().getInstructions()) {
                    String programId = confirmedTransaction.getTransaction().getMessage().getAccountKeys().get((int) instruction.getProgramIdIndex());
                    if (programId.equalsIgnoreCase(JUPITER_PROGRAM_ID.toBase58())) {
                        // if it has OOA and Jup's referrer quote wallet, gg
                        // better to lookup the token account owner, hardcoding top 3 token types for now tho
                        boolean hasReferrer = false, hasOoa = false, hasSrm = false, hasMarket = false;
                        for (long accountIndex : instruction.getAccounts()) {
                            int index = (int) accountIndex;
                            String account = confirmedTransaction.getTransaction().getMessage().getAccountKeys().get(index);
                            if (account.equalsIgnoreCase(SerumUtils.SERUM_PROGRAM_ID_V3.toBase58())) {
                                hasSrm = true;
                            } else if (account.equalsIgnoreCase(ooa)) {
                                hasOoa = true;
                            } else if (account.equalsIgnoreCase(JUPITER_USDC_WALLET.toBase58()) ||
                                    account.equalsIgnoreCase(JUPITER_USDT_WALLET.toBase58()) ||
                                    account.equalsIgnoreCase(JUPITER_WSOL_WALLET.toBase58())) {
                                hasReferrer = true;
                            } else if (account.equalsIgnoreCase(marketId)) {
                                hasMarket = true;
                            }
                        }

                        if (hasOoa && hasReferrer && hasSrm && hasMarket) {
                            // LOGGER.info("FOUND->JUP: " + uniqueKey);
                            jupiterTxMap.put(uniqueKey, Optional.of(signatureInformation.getSignature()));
                            return;
                        }
                    }
                }

            }

            // LOGGER.info("FOUND->NOT-JUP: " + uniqueKey);
            jupiterTxMap.put(uniqueKey, Optional.empty());
        });
        tradeHistoryKeyToFutureMap.put(uniqueKey, future);
        // LOGGER.info("THREAD START: " + uniqueKey);

        return Optional.empty();
    }
}
