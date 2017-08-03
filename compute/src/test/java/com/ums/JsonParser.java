package com.ums;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.json.*;
import unionpay.bussiness.poc.quasirealtimeclearing.flow.UlinkIncre;

/**
 * Created by zhaikaixuan on 02/08/2017.
 */
public class JsonParser {
    @Test
    public void objectToJsonString(){
        UlinkIncre ulink = new UlinkIncre("TRANS_CD_PAY",
                "PAY_ST",
                "TRANS_ST_RSVL",
                "ROUT_INST_ID_CD",
                "TRANS_ST",
                "PROD_STYLE",
                1,
                "MCHNT_ID_PAY",
                false,
                0,
                0);
        JSONObject jo = new JSONObject(ulink);
        System.out.println(jo.toString());
        JSONObject jo2 = new JSONObject(jo.toString());
        System.out.println(jo2.toString());
    }

}
