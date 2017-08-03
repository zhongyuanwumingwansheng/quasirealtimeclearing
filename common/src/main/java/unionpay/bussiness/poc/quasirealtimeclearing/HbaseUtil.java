package unionpay.bussiness.poc.quasirealtimeclearing;

/**
 * Created by root on 8/3/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtil{
    public static void main(String[] args){
        try{
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "testteable");
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c11"), Bytes.toBytes("v11"));
            put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("c21"), Bytes.toBytes("v21"));
            table.put(put);
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}
