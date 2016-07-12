package com.bfm.app.ignite.meetup;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;


public class IgniteComputeTest {


    @Test
    public void colocatedComputeTest() {
        try (Ignite ignite = Ignition.start()) {

            IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache("portCache");

            // Perform a colocated NAV computation
            IgniteCompute compute = ignite.compute().withAsync();
            List<IgniteFuture<Double>> futures = new ArrayList<>();

            LongStream.range(0, 10).forEach(portKey -> {

                // Execute tasks on the nodes where the key is cached
                compute.affinityCall("portCache", portKey, () -> {
                    Portfolio portfolio = cache.get(portKey);
                    List<Position> positions = portfolio.getPositions();
                    double portfolioNAV = positions.stream().mapToDouble(Position::getValue).sum();
                    return portfolioNAV;
                });
                futures.add(compute.future());
            });

            Double aum = futures.stream().mapToDouble(IgniteFuture::get).sum();

            DecimalFormat formatter = new DecimalFormat("#,###.00");
            System.out.println("Aggregate net asset value of 10 portfolios: " + formatter.format(aum));
        }
    }


    @Test
    public void mapReduceTest() {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start()) {

        	IgniteCache<Long, Portfolio> cache = ignite.getOrCreateCache("portCache");

            ComputeTask<String, Double> task = new ComputeTaskAdapter<String, Double>() {

                @Override
                public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws IgniteException {
                    Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

                    for (ClusterNode node : subgrid) {
                        map.put(new ComputeJobAdapter() {
                            @Override
                            public Object execute() {

                                // Load all local portfolio positions
                                ScanQuery<Long, Portfolio> qry = new ScanQuery<Long, Portfolio>((k, p) -> true);
                                qry.setLocal(true);

                                try (QueryCursor<Entry<Long, Portfolio>> cursor = cache.query(qry)) {

                                    Stream<Entry<Long, Portfolio>> stream = StreamSupport.stream(cursor.spliterator(), false);

                                    // Sum the values of all local portfolio positions
                                    Double result = stream.map(e -> e.getValue().getPositions())
                                            .flatMap(p -> p.stream())
                                            .collect(Collectors.summingDouble(Position::getValue));
                                    System.out.println("Returning result " + result + " from " + node);
                                    return result;
                                }
                        }}, node);
                    }
                    return map;
                }

                @Override
                public Double reduce(List<ComputeJobResult> results) throws IgniteException {
                    // Sum the values of positions from all nodes
                    Double result = results.stream().collect(
                            Collectors.summingDouble(ComputeJobResult::getData));
					return result;
                }
            };

            IgniteCompute compute = ignite.compute().withNoFailover();
			Double aum = compute.execute(task, null);

            DecimalFormat formatter = new DecimalFormat("#,###.00");
            System.out.println("Total assets under management: " +  formatter.format(aum));
        }
    }
}
