package ums.bussiness.realtime.reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import ums.bussiness.realtime.model.table.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaikaixuan on 11/08/2017.
 */
public class SaveToIgniteCache {

    private static Config setting = ConfigFactory.load();
    /*
    public IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    public IgniteCache<String, SysGroupItemInfo> sysGroupItemInfoCache = null;
    public IgniteCache<String, SysTxnCdInfo> sysTxnCdInfoCache = null;
    public IgniteCache<String, SysMapItemInfo> sysMapItemInfoCache = null;
    */
    private Ignite ignite = null;
    //private static Ignite ignite = Ignition.start("line/example-ignite.xml");
    //private IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    //private IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    public SaveToIgniteCache() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(setting.getString("tcpDiscoveryIpList").split(",")));
//        ipFinder.setAddresses(Arrays.asList("172.17.1.146:47600..47509"));
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setDiscoverySpi(spi);
        ignite = Ignition.start(cfg);
    }

    public void parseTableAndSave2Ignite(){
        try {
            String tempString = "";

            //read sysGroupItemInfo
            System.out.println("Begin Read sysGroupItemInfo");
            FileInputStream sysGroupItemInfoFile = new FileInputStream(setting.getString("SysGroupItemInfo.SysGroupItemInfoLoc"));
            InputStreamReader sysGroupItemInfoinputFileReader = new InputStreamReader(sysGroupItemInfoFile, setting.getString("coding"));
            BufferedReader sysGroupItemInforeader = new BufferedReader(sysGroupItemInfoinputFileReader);
            while ((tempString = sysGroupItemInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");

                SysGroupItemInfo item = new SysGroupItemInfo(splitColumns[setting.getInt("SysGroupItemInfo.instIdIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.batDateIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.groupIdIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.groupTypeIdIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.itemIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.becomeEffectiveDateIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.lostEffectiveDateIndex")],
                        Integer.parseInt(splitColumns[setting.getInt("SysGroupItemInfo.rcdVerIndex")]),
                        splitColumns[setting.getInt("SysGroupItemInfo.addDatetimeIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.addUserIdIndex")],
                        "",//splitColumns[setting.getInt("SysGroupItemInfo.updDatetimeIndex")],
                        "");//splitColumns[setting.getInt("SysGroupItemInfo.updUserIdIndex")]);
                String keyValue = splitColumns[0] + splitColumns[1] + splitColumns[2] + splitColumns[3] + splitColumns[4];
                CacheConfiguration<String, SysGroupItemInfo> sysGroupItemInfoCfg = new CacheConfiguration<>();
                sysGroupItemInfoCfg.setIndexedTypes(String.class, SysGroupItemInfo.class);
                sysGroupItemInfoCfg.setCacheMode(CacheMode.REPLICATED);
                sysGroupItemInfoCfg.setName("SysGroupItemInfo");
                sysGroupItemInfoCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//                sysGroupItemInfoCfg.setQueryParallelism(10000);
//                sysGroupItemInfoCfg.setMaxConcurrentAsyncOperations(10000);
                IgniteCache<String, SysGroupItemInfo> sysGroupItemInfoCache = ignite.getOrCreateCache(sysGroupItemInfoCfg);
                System.out.println("SysGroupItemInfo:  " + keyValue);
                sysGroupItemInfoCache.put(keyValue, item);
            }
            sysGroupItemInforeader.close();

            //read sysTxnCdInfo
            System.out.println("Begin Read sysTxnCdInfo");
            FileInputStream sysTxnCdInfoFile = new FileInputStream(setting.getString("SysTxnCdInfo.SysTxnCdInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader sysTxnCdInfoinputFileReader = new InputStreamReader(sysTxnCdInfoFile, setting.getString("coding"));
            BufferedReader sysTxnCdInforeader = new BufferedReader(sysTxnCdInfoinputFileReader);
            // 一次读入一行，直到读入null为文件结束

            while ((tempString = sysTxnCdInforeader.readLine()) != null) {
                String[] splitColumns = tempString.replace("|||", "| | |").replace("||", "| |").trim().split("\\|");
                SysTxnCdInfo item = new SysTxnCdInfo();
                if (splitColumns.length == 12) {
                    item = new SysTxnCdInfo(splitColumns[setting.getInt("SysTxnCdInfo.txnKeyIndex")],
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeIndex")],
                            splitColumns[setting.getInt("SysTxnCdInfo.txnDesIndex")],
                            splitColumns[setting.getInt("SysTxnCdInfo.bmsTxnCodeIndex")],
                            splitColumns[setting.getInt("SysTxnCdInfo.settFlgIndex")],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.dcFlgIndex")]),
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeGrp")],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.rcdVerIndex")]),
                            splitColumns[setting.getInt("SysTxnCdInfo.addDatetimeIndex")],
                            splitColumns[setting.getInt("SysTxnCdInfo.addUserIdIndex")],
                            "",//splitColumns[setting.getInt("SysTxnCdInfo.updDatetimeIndex")],
                            "");//splitColumns[setting.getInt("SysTxnCdInfo.updUserIdIndex")]);
                }
                else if (splitColumns.length == 14){
                    item = new SysTxnCdInfo(splitColumns[0]+"|"+splitColumns[1]+"|"+splitColumns[2],
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeIndex")+2],
                            splitColumns[setting.getInt("SysTxnCdInfo.txnDesIndex")+2],
                            splitColumns[setting.getInt("SysTxnCdInfo.bmsTxnCodeIndex")+2],
                            splitColumns[setting.getInt("SysTxnCdInfo.settFlgIndex")+2],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.dcFlgIndex") +2]),
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeGrp")+2],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.rcdVerIndex")+2]),
                            splitColumns[setting.getInt("SysTxnCdInfo.addDatetimeIndex")+2],
                            splitColumns[setting.getInt("SysTxnCdInfo.addUserIdIndex")+2],
                            "",//splitColumns[setting.getInt("SysTxnCdInfo.updDatetimeIndex")],
                            "");//splitColumns[setting.getInt("SysTxnCdInfo.updUserIdIndex")]);
                }
                else {
                    int offset = splitColumns.length - 12;
                    String first = "";
                    List<String> firstField = Arrays.asList(splitColumns).subList(0, offset+1);
                    boolean flag = false;
                    for (String field:firstField){
                        if (flag){
                            first += "|";
                        }else {
                            flag = true;
                        }
                        first+=field;
                    }
                    item = new SysTxnCdInfo(first.toString(),
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeIndex")+offset],
                            splitColumns[setting.getInt("SysTxnCdInfo.txnDesIndex")+offset],
                            splitColumns[setting.getInt("SysTxnCdInfo.bmsTxnCodeIndex")+offset],
                            splitColumns[setting.getInt("SysTxnCdInfo.settFlgIndex")+offset],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.dcFlgIndex") +offset]),
                            splitColumns[setting.getInt("SysTxnCdInfo.txnCodeGrp")+offset],
                            Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.rcdVerIndex")+offset]),
                            splitColumns[setting.getInt("SysTxnCdInfo.addDatetimeIndex")+offset],
                            splitColumns[setting.getInt("SysTxnCdInfo.addUserIdIndex")+offset],
                            "",//splitColumns[setting.getInt("SysTxnCdInfo.updDatetimeIndex")],
                            "");//splitColumns[setting.getInt("SysTxnCdInfo.updUserIdIndex")]);
                }
                String keyValue = item.getTxnKey();
                CacheConfiguration<String, SysTxnCdInfo> sysTxnCdInfoCfg = new CacheConfiguration<>();
                sysTxnCdInfoCfg.setIndexedTypes(String.class, SysTxnCdInfo.class);
                sysTxnCdInfoCfg.setCacheMode(CacheMode.REPLICATED);
                sysTxnCdInfoCfg.setName("SysTxnCdInfo");
                sysTxnCdInfoCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//                sysTxnCdInfoCfg.setQueryParallelism(10000);
//                sysTxnCdInfoCfg.setMaxConcurrentAsyncOperations(10000);
                IgniteCache<String, SysTxnCdInfo> sysTxnCdInfoCache = ignite.getOrCreateCache(sysTxnCdInfoCfg);
                System.out.println("SysTxnCdInfo:  " + keyValue);
                sysTxnCdInfoCache.put(keyValue, item);
            }
            sysTxnCdInforeader.close();


            //read sysMapItemInfo
            System.out.println("Begin Read sysMapItemInfo");
            FileInputStream sysMapItemInfoFile = new FileInputStream(setting.getString("SysMapItemInfo.SysMapItemInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader sysMapItemInfoinputFileReader = new InputStreamReader(sysMapItemInfoFile, setting.getString("coding"));
            BufferedReader sysMapItemInforeader = new BufferedReader(sysMapItemInfoinputFileReader);
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = sysMapItemInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");
                SysMapItemInfo item = new SysMapItemInfo(splitColumns[setting.getInt("SysMapItemInfo.mapIdIndex")],
                        splitColumns[setting.getInt("SysMapItemInfo.srcItemIndex")],
                        splitColumns[setting.getInt("SysMapItemInfo.mapResultIndex")]);
                item.setTypeId(splitColumns[setting.getInt("SysMapItemInfo.typeId")]);
                String keyValue = splitColumns[0]+splitColumns[1]+splitColumns[2]+splitColumns[3]+splitColumns[4];
                CacheConfiguration<String, SysMapItemInfo> sysMapItemInfoCfg = new CacheConfiguration<>();
                sysMapItemInfoCfg.setIndexedTypes(String.class, SysMapItemInfo.class);
                sysMapItemInfoCfg.setCacheMode(CacheMode.REPLICATED);
                sysMapItemInfoCfg.setName("SysMapItemInfo");
                sysMapItemInfoCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//                sysMapItemInfoCfg.setQueryParallelism(10000);
//                sysMapItemInfoCfg.setMaxConcurrentAsyncOperations(10000);
                IgniteCache<String, SysMapItemInfo> sysMapItemInfoCache = ignite.getOrCreateCache(sysMapItemInfoCfg);
                System.out.println("SysMapItemInfo:  " + keyValue);
                sysMapItemInfoCache.put(keyValue, item);
            }
            sysMapItemInforeader.close();


            //read bmsStInfo
            System.out.println("Begin Read bmsStInfo");
            FileInputStream bmsStInfoFile = new FileInputStream(setting.getString("BmsStInfo.BmsStInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader bmsStInfoinputFileReader = new InputStreamReader(bmsStInfoFile, setting.getString("coding"));
            BufferedReader bmsStInforeader = new BufferedReader(bmsStInfoinputFileReader);
          // 一次读入一行，直到读入null为文件结束
            while ((tempString = bmsStInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");
                System.out.println(splitColumns[setting.getInt("BmsStInfo.merNoIndex")]);
                BmsStInfo item = new BmsStInfo("",
                        splitColumns[setting.getInt("BmsStInfo.merNoIndex")],
                        splitColumns[setting.getInt("BmsStInfo.mapMainIndex")],
                        Integer.parseInt(splitColumns[setting.getInt("BmsStInfo.apptypeidIndex")]),
                        splitColumns[setting.getInt("BmsStInfo.creditCalTypeIndex")],
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.creditCalcRateIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.creditCalAmtIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.creditMinAmtIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.creditMaxAmtIndex")]));
                String keyValue = splitColumns[0] + splitColumns[1];
                CacheConfiguration<String, BmsStInfo> bmsStInfoCfg = new CacheConfiguration<>();
                bmsStInfoCfg.setIndexedTypes(String.class, BmsStInfo.class);
                bmsStInfoCfg.setCacheMode(CacheMode.REPLICATED);
                bmsStInfoCfg.setName("BmsStInfo");
                bmsStInfoCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//                bmsStInfoCfg.setQueryParallelism(10000);
//                bmsStInfoCfg.setMaxConcurrentAsyncOperations(10000);
                IgniteCache<String, BmsStInfo> bmsStInfoCache = ignite.getOrCreateCache(bmsStInfoCfg);
                System.out.println("BmsStInfo:  " + keyValue);
                bmsStInfoCache.put(keyValue, item);
            }
            bmsStInforeader.close();

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }
    public static void main(String args[]){
        SaveToIgniteCache cache = new SaveToIgniteCache();
        cache.parseTableAndSave2Ignite();
        /*
        System.out.println(cache.bmsStInfoCache.get("89863015998936586").toString());
        System.out.println(cache.sysGroupItemInfoCache.get("4802000020170724QMJF_NEW02898310145114004").toString());
        System.out.println(cache.sysMapItemInfoCache.get("1898350555110002,35059294").toString());
        System.out.println(cache.sysTxnCdInfoCache.get("E07").toString());
        */
    }


}
