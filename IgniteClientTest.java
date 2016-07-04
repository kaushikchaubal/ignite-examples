package com.ignite.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;



public class IgniteClientTest {

	private static final String CONFIG = "config/default-config.xml";
	
	public static final long ASSET_MV = 500;
    public static final long NUM_PORTFOLIOS = 10000;
    public static final long POS_PER_PORT = 100;

    @Test
    // TODO - diagram illustrating object graph and node backups
    public void loadData() {
        Ignition.setClientMode(true);               
        try (Ignite ignite = Ignition.start()) {
            
            // Configure partitioned cache
            CacheConfiguration<Long, Portfolio> cfg = new CacheConfiguration<Long, Portfolio>("portCache"); 
            cfg.setIndexedTypes(Long.class, Portfolio.class); 
            cfg.setCacheMode(CacheMode.PARTITIONED);
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
				System.out.println("Total AUM: " + cursor.getAll());
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
	        
            cache.put(1L, new Portfolio(1L, "Portfolio1", "First Portfolio"));
            cache.put(2L, new Portfolio(2L, "Portfolio2", "Second Portfolio"));
            cache.put(3L, new Portfolio(2L, "Portfolio3", "Third Portfolio"));
            cache.put(4L, new Portfolio(3L, "Portfolio4", "Fourth Portfolio"));

			TextQuery<Long, Portfolio> txt 
				= new TextQuery<>(Portfolio.class, "name:AL* AND description:global~");
			
			try (QueryCursor<Entry<Long, Portfolio>> cursor = cache.query(txt)) {
				System.out.println("Query result: " + cursor.getAll());
			}
		}
	}
	
	@Test
	public void mapReduceTest() {
        Ignition.setClientMode(true);               
		try (Ignite ignite = Ignition.start()) {
			
		    ComputeTask<String, Double> task = new ComputeTaskAdapter<String, Double>() {
		
		        @Override
		        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws IgniteException {
		            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());
		            
		            for (ClusterNode node : subgrid) {
		                map.put(new ComputeJobAdapter() {
		                    @Override
		                    public Object execute() {
		                        
		                    	IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache("portCache");
		                    	
		                    	// Load all local portfolio positions 
		                        ScanQuery<Long, Portfolio> qry = new ScanQuery<Long, Portfolio>((k, p) -> true);
		                        qry.setLocal(true);
		                        
		                        try (QueryCursor<Entry<Long, Portfolio>> cursor = cache.query(qry)) {
		                            
		                            Stream<Entry<Long, Portfolio>> stream = StreamSupport.stream(cursor.spliterator(), false);
		                            
		                            // Sum the values of all local portfolio positions 
		                            return stream.map(e -> e.getValue().getPositions())
		                            		.flatMap(p -> p.stream())
		                            		.collect(Collectors.summingDouble(Position::getValue));
		                    }
		                }}, node);
		            }
		            return map;
		        }
		
		        @Override
		        public Double reduce(List<ComputeJobResult> results) throws IgniteException {
		        	// Sum the values of positions from all nodes
		        	return results.stream().collect(
		        			Collectors.summingDouble(ComputeJobResult::getData));
		        }
		    };
		
		    Double aum = ignite.compute().execute(task, null);
		    
		    DecimalFormat myFormatter = new DecimalFormat("#,###.00");
		    System.out.println("AUM: " +  myFormatter.format(aum));
		}
	}

	


}
