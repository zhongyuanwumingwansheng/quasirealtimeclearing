package unionpay.bussiness.poc.quasirealtimeclearing.flow;

import com.ums.QueryRelatedProperty;
import com.ums.SendMessage;

/**
 * Created by root on 7/31/17.
 */

import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;
public class Ulink {
    /**
     * 借贷标记
     * 1: 借记
     * -1： 贷记
     */
    private int dcFlag;
    /**
     * 清算标志
     * 1: 清算
     * -1： 不清算
     */
    private int clearingFlag;

    //TODO,eliminate Hard Code
    public Ulink() {
        this.dcFlag = 1;
        this.clearingFlag = 1;
    }

    public Ulink(int dcFlag, int clearingFlag) {
        this.dcFlag = dcFlag;
        this.clearingFlag = clearingFlag;
    }

    public void setDcFlag(int dcFlag) {
        this.dcFlag = dcFlag;
    }

    public void setClearingFlag(int clearingFlag) {
        this.clearingFlag = clearingFlag;
    }

    public int getDcFlag() {
        return dcFlag;
    }

    public int getClearingFlag() {
        return clearingFlag;
    }

    public String queryProperty(QueryRelatedProperty qRelPro, String tableName, String targetCol, String sourceColName, String sourceColValue){
        return qRelPro.queryProperty(tableName, targetCol, sourceColName, sourceColValue);
    }
    public void sendMessage(SendMessage sMsg,String message){
         sMsg.sendMessage(message);
    }
}
