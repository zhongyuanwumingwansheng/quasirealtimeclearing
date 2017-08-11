package ums.bussiness.realtime.reader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.producer.KeyedMessage;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import ums.bussiness.realtime.model.table.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by zhaikaixuan on 11/08/2017.
 */
public class SaveToIgniteCache {
    private static Config setting = ConfigFactory.load();
    //public IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    public IgniteCache<String, SysGroupItemInfo> sysGroupItemInfoCache = null;
    public IgniteCache<String, SysTxnCdInfo> sysTxnCdInfoCache = null;
    public IgniteCache<String, SysMapItemInfo> sysMapItemInfoCache = null;
    private static Ignite ignite = Ignition.start();
    //private IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    //private IgniteCache<String, BmsStInfo> bmsStInfoCache = null;
    public SaveToIgniteCache() throws IgniteException{

        {
            System.out.println();
            System.out.println(">>> Ignite started.");
            //cache configuration for bmsStInfo
            CacheConfiguration<String, BmsStInfo> bmsStInfoCfg = new CacheConfiguration<>();
            bmsStInfoCfg.setCacheMode(CacheMode.LOCAL);
            bmsStInfoCfg.setName("BmsStInfo");
            IgniteCache<String, BmsStInfo> bmsStInfoCache = ignite.getOrCreateCache(bmsStInfoCfg);
            /*
            //cache configuration for sysGroupItemInfo
            CacheConfiguration<String, SysGroupItemInfo> sysGroupItemInfoCfg = new CacheConfiguration<>();
            sysGroupItemInfoCfg.setCacheMode(CacheMode.LOCAL);
            sysGroupItemInfoCfg.setName("SysGroupItemInfo");
            sysGroupItemInfoCache = ignite.getOrCreateCache(sysGroupItemInfoCfg);
            //cache configuration for sysTxnCdInfo
            CacheConfiguration<String, SysTxnCdInfo> sysTxnCdInfoCfg = new CacheConfiguration<>();
            sysTxnCdInfoCfg.setCacheMode(CacheMode.LOCAL);
            sysTxnCdInfoCfg.setName("SysTxnCdInfo");
            sysTxnCdInfoCache = ignite.getOrCreateCache(sysTxnCdInfoCfg);
            //cache configuration for SysMapItemInfo
            CacheConfiguration<String, SysMapItemInfo> sysMapItemInfoCfg = new CacheConfiguration<>();
            sysMapItemInfoCfg.setCacheMode(CacheMode.LOCAL);
            sysMapItemInfoCfg.setName("SysMapItemInfo");
            sysMapItemInfoCache = ignite.getOrCreateCache(sysMapItemInfoCfg);
            */
        }
    }

    public void parseTableAndSave2Ignite(){
        try {
            //read bmsStInfo
            FileInputStream bmsStInfoFile = new FileInputStream(setting.getString("BmsStInfo.BmsStInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader bmsStInfoinputFileReader = new InputStreamReader(bmsStInfoFile);
            BufferedReader bmsStInforeader = new BufferedReader(bmsStInfoinputFileReader);
            String tempString;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = bmsStInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");
                BmsStInfo item = new BmsStInfo(splitColumns[setting.getInt("BmsStInfo.merIdIndex")],
                        splitColumns[setting.getInt("BmsStInfo.merIdIndex")],
                        splitColumns[setting.getInt("BmsStInfo.merIdIndex")],
                        Integer.parseInt(splitColumns[setting.getInt("BmsStInfo.merIdIndex")]),
                        splitColumns[setting.getInt("BmsStInfo.merIdIndex")],
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.merIdIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.merIdIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.merIdIndex")]),
                        Double.parseDouble(splitColumns[setting.getInt("BmsStInfo.merIdIndex")]));
                String keyValue = splitColumns[0] + splitColumns[1];
                CacheConfiguration<String, BmsStInfo> bmsStInfoCfg = new CacheConfiguration<>();
                bmsStInfoCfg.setCacheMode(CacheMode.LOCAL);
                bmsStInfoCfg.setName("BmsStInfo");
                IgniteCache<String, BmsStInfo> bmsStInfoCache = ignite.getOrCreateCache(bmsStInfoCfg);
                bmsStInfoCache.put(keyValue, item);
            }
            bmsStInforeader.close();

            //read sysGroupItemInfo
            FileInputStream sysGroupItemInfoFile = new FileInputStream(setting.getString("SysGroupItemInfo.SysGroupItemInfoLoc"));
            InputStreamReader sysGroupItemInfoinputFileReader = new InputStreamReader(sysGroupItemInfoFile);
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
                        splitColumns[setting.getInt("SysGroupItemInfo.updDatetimeIndex")],
                        splitColumns[setting.getInt("SysGroupItemInfo.updUserIdIndex")]);
                String keyValue = splitColumns[0] + splitColumns[1] + splitColumns[2] + splitColumns[3] + splitColumns[4];
                //sysGroupItemInfoCache.put(keyValue, item);
            }
            sysGroupItemInforeader.close();

            //read sysTxnCdInfo
            FileInputStream sysTxnCdInfoFile = new FileInputStream(setting.getString("SysTxnCdInfo.SysTxnCdInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader sysTxnCdInfoinputFileReader = new InputStreamReader(sysTxnCdInfoFile);
            BufferedReader sysTxnCdInforeader = new BufferedReader(sysTxnCdInfoinputFileReader);
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = sysTxnCdInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");
                SysTxnCdInfo item = new SysTxnCdInfo(splitColumns[setting.getInt("SysTxnCdInfo.txnKeyIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.txnCodeIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.txnDesIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.bmsTxnCodeIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.settFlgIndex")],
                        Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.dcFlgIndex")]),
                        splitColumns[setting.getInt("SysTxnCdInfo.txnCodeGrp")],
                        Integer.parseInt(splitColumns[setting.getInt("SysTxnCdInfo.rcdVerIndex")]),
                        splitColumns[setting.getInt("SysTxnCdInfo.addDatetimeIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.addUserIdIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.updDatetimeIndex")],
                        splitColumns[setting.getInt("SysTxnCdInfo.updUserIdIndex")]);
                String keyValue = splitColumns[0];
                //sysTxnCdInfoCache.put(keyValue, item);
            }
            sysTxnCdInforeader.close();

            //read sysMapItemInfo
            FileInputStream sysMapItemInfoFile = new FileInputStream(setting.getString("BmsStInfo.BmsStInfoLoc"));
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            InputStreamReader sysMapItemInfoinputFileReader = new InputStreamReader(sysMapItemInfoFile);
            BufferedReader sysMapItemInforeader = new BufferedReader(sysMapItemInfoinputFileReader);
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = sysMapItemInforeader.readLine()) != null) {
                String[] splitColumns = tempString.trim().split("\\|");
                SysMapItemInfo item = new SysMapItemInfo(splitColumns[setting.getInt("SysMapItemInfo.mapIdIndex")],
                        splitColumns[setting.getInt("SysMapItemInfo.srcItemIndex")],
                        splitColumns[setting.getInt("SysMapItemInfo.mapResultIndex")]);
                String keyValue = splitColumns[0];
                //sysMapItemInfoCache.put(keyValue, item);
            }
            sysMapItemInforeader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }
    public static void main(String args[]){
        SaveToIgniteCache cache = new SaveToIgniteCache();
        cache.parseTableAndSave2Ignite();

        //System.out.println(cache.bmsStInfoCache.get("89863015998936586").toString());
        System.out.println(cache.sysGroupItemInfoCache.get("4802000020170724QMJF_NEW02898310145114004").toString());
        System.out.println(cache.sysMapItemInfoCache.get("1898350555110002,35059294").toString());
        System.out.println(cache.sysTxnCdInfoCache.get("E07").toString());
    }


}
