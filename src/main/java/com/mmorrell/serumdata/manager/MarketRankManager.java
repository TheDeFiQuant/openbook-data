package com.mmorrell.serumdata.manager;

import ch.openserum.serum.model.Market;
import ch.openserum.serum.model.SerumUtils;
import com.mmorrell.serumdata.model.Token;
import com.mmorrell.serumdata.util.MarketUtil;
import org.p2p.solanaj.core.PublicKey;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class MarketRankManager {

    private static final int RANK_PLACEHOLDER = 9999999;

    // Top tokens list, for quicker resolution from symbol.
    private static final Map<String, Token> TOP_TOKENS = Map.of(
            "SOL", new Token(SerumUtils.WRAPPED_SOL_MINT),
            "USDC", new Token(MarketUtil.USDC_MINT),
            "USDT", new Token(MarketUtil.USDT_MINT)
    );

    private final MarketManager marketManager;
    private final TokenManager tokenManager;

    public MarketRankManager(MarketManager marketManager, TokenManager tokenManager) {
        this.marketManager = marketManager;
        this.tokenManager = tokenManager;
    }

    /**
     * Returns rank of tokenMint, the highest rank is 1, based on # of Serum markets
     * @param tokenMint mint to rank based on # of serum markets
     * @return serum market rank for the given token
     */
    public int getMarketRankOfToken(PublicKey tokenMint) {
        Map<PublicKey, Integer> marketCounts = new HashMap<>();

        marketManager.getMarketMapCache().forEach((token, markets) -> marketCounts.put(token, markets.size()));

        List<Map.Entry<PublicKey, Integer>> list = new ArrayList<>(marketCounts.entrySet());
        list.sort(Map.Entry.comparingByValue((o1, o2) -> o2 - o1));

        Map<PublicKey, Integer> marketRanks = new HashMap<>();
        for (int i = 0; i < list.size(); i++) {
            marketRanks.put(list.get(i).getKey(), i + 1);
        }

        return marketRanks.getOrDefault(tokenMint, RANK_PLACEHOLDER);
    }

    public Optional<Market> getMostActiveMarket(PublicKey baseMint) {
        List<Market> markets = marketManager.getMarketsByMint(baseMint);
        if (markets.size() < 1) {
            return Optional.empty();
        }

        // sort by base deposits
        markets.sort(Comparator.comparingLong(Market::getBaseDepositsTotal).reversed());

        // prefer USDC over other pairs if 2 top pairs are XYZ / USDC
        if (markets.size() > 1) {
            Market firstMarket = markets.get(0);
            Market secondMarket = markets.get(1);

            // if first pair isn't USDC quoted, and second pair is, move it to first place
            if (!firstMarket.getQuoteMint().equals(MarketUtil.USDC_MINT) &&
                    secondMarket.getQuoteMint().equals(MarketUtil.USDC_MINT)) {
                markets.set(0, secondMarket);
                markets.set(1, firstMarket);
            }
        }

        return Optional.ofNullable(markets.get(0));
    }

    public Optional<Market> getMostActiveMarket(PublicKey baseMint, PublicKey quoteMint) {
        List<Market> markets = marketManager.getMarketsByMint(baseMint);
        if (markets.size() < 1) {
            return Optional.empty();
        }

        // sort by base deposits
        markets.sort(Comparator.comparingLong(Market::getBaseDepositsTotal).reversed());
        for (Market market : markets) {
            if (market.getQuoteMint().equals(quoteMint)) {
                return Optional.of(market);
            }
        }

        return Optional.empty();
    }

    /**
     * Returns lightweight token (address only) if given symbol has an active serum market.
     * @param symbol e.g. SOL or USDC or RAY
     * @return most active token for given symbol
     */
    public Optional<Token> getMostSerumActiveTokenBySymbol(String symbol) {
        if (TOP_TOKENS.containsKey(symbol)) {
            return Optional.of(TOP_TOKENS.get(symbol));
        }

        List<Token> possibleBaseTokens = tokenManager.getTokensBySymbol(symbol);
        List<Market> activeMarkets = new ArrayList<>();

        for (Token baseToken : possibleBaseTokens) {
            // compile list of markets, return one with most fees accrued.
            Optional<Market> optionalMarket = getMostActiveMarket(baseToken.getPublicKey());
            optionalMarket.ifPresent(activeMarkets::add);
        }
        activeMarkets.sort(Comparator.comparingLong(Market::getQuoteFeesAccrued).reversed());

        if (activeMarkets.size() > 0) {
            return tokenManager.getTokenByMint(activeMarkets.get(0).getBaseMint());
        } else {
            return Optional.empty();
        }
    }
}
