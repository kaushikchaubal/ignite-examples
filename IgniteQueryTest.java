package com.bfm.app.ignite.meetup;

import java.util.ArrayList;
import java.util.List;

import javax.cache.Cache;
import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;



public class IgniteQueryTest {

	private static final long ASSET_MV = 500;
	private static final long NUM_PORTFOLIOS = 10000;
	private static final long POS_PER_PORT = 100;

    @Test
    public void loadData() {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start()) {

            // Configure partitioned cache
            CacheConfiguration<Long, Portfolio> cfg = new CacheConfiguration<Long, Portfolio>("portCache");
            cfg.setIndexedTypes(Long.class, Portfolio.class);
            cfg.setBackups(1);
            ignite.getOrCreateCache(cfg);

            System.out.println("Loading data");

            // Put 10k portfolios into the cache
            try (IgniteDataStreamer<Long, Portfolio> dataStreamer = ignite.dataStreamer("portCache")) {

                for (long portIdx=0; portIdx<NUM_PORTFOLIOS; portIdx++) {

                    // Each portfolio has a list of 100 positions - each with a market value of GBP500
                    List<Position> positions = new ArrayList<Position>();
                    for (long posIdx=0; posIdx<POS_PER_PORT; posIdx++) {
                        Position pos = new Position(posIdx, portIdx, "CUSIP"+posIdx, ASSET_MV);
                        positions.add(pos);
                    }
                    Portfolio portfolio = new Portfolio(portIdx, "PORT"+portIdx, ASSET_MV * POS_PER_PORT, positions);
                    dataStreamer.addData(portIdx, portfolio);
                }
            }
            System.out.println("Cache data load complete");
        }
    }


	@Test
	public void sqlQueryTest() {
        Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) {

	        IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache("portCache");

			SqlFieldsQuery countSql = new SqlFieldsQuery("select count(*) from portfolio");
			try (QueryCursor<List<?>> cursor = cache.query(countSql)) {
				System.out.println("Cache entries: " + cursor.getAll());
			}

			SqlFieldsQuery aumSql = new SqlFieldsQuery("select sum(nav) from portfolio");
			try (QueryCursor<List<?>> cursor = cache.query(aumSql)) {
				System.out.println("Total value of assets under management: " + cursor.getAll());
			}
		}
	}


    @Test
    public void continuousQueryTest() {
        try (Ignite ignite = Ignition.start()) {
            ignite.destroyCache("myCache");
            IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache("myCache");

            ContinuousQuery<Long, Portfolio> qry = new ContinuousQuery<>();

            // Optional initial query to select all entries with NAV>1000
            qry.setInitialQuery(new ScanQuery<Long, Portfolio>((k, v) -> v.getNav() > 1000));

            // Remotely filter portfolios with small NAVs
            qry.setRemoteFilterFactory(() -> e -> e.getValue().getNav() > 1000);

            // Callback for update notifications
            qry.setLocalListener((evts) -> evts
                    .forEach(e -> System.out.println("Listener Event: " + e.getEventType()
                            + " key=" + e.getKey() + ", val=" + e.getValue())));

            // Seed the cache with two entries
            cache.put(1L, new Portfolio(1L, "P1", 1000, null));
            cache.put(2L, new Portfolio(2L, "P2", 2000, null));

            // Execute the query
            try (QueryCursor<Cache.Entry<Long, Portfolio>> cur = cache.query(qry)) {
            	System.out.println("Initial query result: " + cur.getAll());

	            // Adding items causes notifications
	            cache.put(2L, new Portfolio(2L, "P2", 3000, null));
	            cache.put(3L, new Portfolio(3L, "P3", 4000, null));
            }
        }
    }


    @Test
    public void textQueryTest() {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start()) {

            CacheConfiguration<Long, Portfolio> cfg = new CacheConfiguration<Long, Portfolio>("portCache2");
            cfg.setIndexedTypes(Long.class, Portfolio.class);
            IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache(cfg);

            cache.put(1L, new Portfolio(1L, "AZMOTT", "Consensus Fund"));
            cache.put(2L, new Portfolio(2L, "AZ5050", "50:50 Global Equity Fund"));
            cache.put(3L, new Portfolio(2L, "AZ4060", "40:60 Global Equity Fund"));
            cache.put(4L, new Portfolio(3L, "COMAAT", "Consolidated Pension Plan"));

            TextQuery<Long, Portfolio> txt
                = new TextQuery<>(Portfolio.class, "name:AZ* AND description:global~");

            try (QueryCursor<Entry<Long, Portfolio>> cursor = cache.query(txt)) {
                System.out.println("Query result: " + cursor.getAll());
            }
        }
    }

}
