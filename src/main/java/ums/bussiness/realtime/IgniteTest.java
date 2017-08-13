package ums.bussiness.realtime;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import ums.bussiness.realtime.model.table.BmsStInfo;

import java.util.List;

public class IgniteTest {

    public static void main(String args[]){
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
//        igniteConfiguration.setClientMode(true);
        Ignite ignite = Ignition.start(igniteConfiguration);
        CacheConfiguration<String, BmsStInfo> bmsStInfoCfg = new CacheConfiguration<>();
        bmsStInfoCfg.setIndexedTypes(String.class,BmsStInfo.class);
        bmsStInfoCfg.setName("BmsStInfo");
        IgniteCache<String, BmsStInfo> bmsStInfoCache = ignite.getOrCreateCache(bmsStInfoCfg);
        String sql = "\"BmsStInfo\".BmsStInfo.mappMain > 0";
        IgniteCache<String, BmsStInfo> cache = ignite.cache("BmsStInfo");
        BmsStInfo result = bmsStInfoCache.get("89863015998933786");
        SqlQuery qry = new SqlQuery<String, BmsStInfo>(BmsStInfo.class, sql);
        // Enable distributed joins for query.
        qry.setDistributedJoins(true);
        qry.setReplicatedOnly(true);
        print("result:" + result.getApptypeId() + "----" + result.getCreditCalcRate());
        List<List<?>> queryResult = bmsStInfoCache.query(qry).getAll();
        print(queryResult);
        print("Following people are 'ApacheIgnite' employees (distributed join): ", bmsStInfoCache.query(qry).getAll());
        ignite.close();
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next.toString());
    }
}
