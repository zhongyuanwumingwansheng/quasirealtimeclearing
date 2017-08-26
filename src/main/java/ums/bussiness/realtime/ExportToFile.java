package ums.bussiness.realtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import ums.bussiness.realtime.model.table.BmsStInfo;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import ums.bussiness.realtime.model.table.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by zhaikaixuan on 23/08/2017.
 */
public class ExportToFile {
    private static Config setting = ConfigFactory.load();
    private static Ignite ignite = null;

    public ExportToFile(){
        if (ignite == null){
            IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
            //igniteConfiguration.setMemoryConfiguration(memoryConfiguration);
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            ipFinder.setAddresses(java.util.Arrays.asList(setting.getString("tcpDiscoveryIpList").split(",")));
            //ipFinder.setAddresses(Arrays.asList("172.30.252.210:47600..47609"));
            spi.setIpFinder(ipFinder);
            igniteConfiguration.setDiscoverySpi(spi);
            igniteConfiguration.setClientMode(true);
            ignite = Ignition.start(igniteConfiguration);
        }

    }
    public void export(String cacheName){
        CacheConfiguration<String, Double> summaryCfg = new CacheConfiguration<>();
        summaryCfg.setIndexedTypes(String.class, Double.class);
        summaryCfg.setName(cacheName);
        //IgniteCache<String, Double> summaryCache_pre = ignite.getOrCreateCache(summaryCfg);
        //summaryCache_pre.put("hello", 10.1);
        IgniteCache<String, Double> summaryCache = ignite.getOrCreateCache(summaryCfg);
        String saveLocation = setting.getString("resultSaveLocation");
        try{
            System.out.println("Begin");
            File file =new File(saveLocation + cacheName);
            //if file doesn't exists, then create it
            if(!file.exists()){
                file.createNewFile();
            }

            //true = append file
            FileWriter fileWriter = new FileWriter(file,false);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            for (javax.cache.Cache.Entry<String, Double> igniteEntry:summaryCache){
                System.out.println(igniteEntry.getKey() + ", " + igniteEntry.getValue().toString());
                bufferWriter.write(igniteEntry.getKey() + ", " + igniteEntry.getValue().toString());
                bufferWriter.newLine();
            }
            bufferWriter.close();

            System.out.println("Done");

        }catch(IOException e){
            e.printStackTrace();
        }

    }
    public static void main(String args[]){
//        String incrementalSummary = "IncrementalSummary";
        String normalSummary = "NormalSummary";
        ExportToFile exportToFile = new ExportToFile();
//        exportToFile.export(incrementalSummary);
        exportToFile.export(normalSummary);
    }
}
