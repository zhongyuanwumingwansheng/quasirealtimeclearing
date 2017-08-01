package unionpay.bussiness.poc.quasirealtimeclearing.flow;

import com.ums.QueryRelatedProperty;
import com.ums.SendMessage;

/**
 * Created by root on 7/31/17.
 */
public class Ulink {
    private String dcFlag = null;

    private String clearingFlag = "-2";

    public void setDcFlag(String dcFlag) {
        this.dcFlag = dcFlag;
    }

    public void setClearingFlag(String clearingFlag) {
        this.clearingFlag = clearingFlag;
    }

    public String getDcFlag() {
        return dcFlag;
    }

    public String getClearingFlag() {
        return clearingFlag;
    }

    public String queryProperty(QueryRelatedProperty qRelPro, String tableName, String targetCol, String sourceColName, String sourceColValue){
        return qRelPro.queryProperty(tableName, targetCol, sourceColName, sourceColValue);
    }
    public void sendMessage(SendMessage sMsg,String message){
         sMsg.sendMessage(message);
    }
}
