package unionpay.bussiness.poc.quasirealtimeclearing;

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
        UlinkIncre ulink = new UlinkIncre();
        ulink.setdRsvd6(1);
        JSONObject jo = new JSONObject(ulink);
        System.out.println(jo.toString());
        JSONObject jo2 = new JSONObject(jo.toString());
        System.out.println(jo2.toString());
    }

}
