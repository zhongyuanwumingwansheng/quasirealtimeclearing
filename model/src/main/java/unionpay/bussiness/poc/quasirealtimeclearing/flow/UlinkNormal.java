package unionpay.bussiness.poc.quasirealtimeclearing.flow;

/**
 * Created by root on 7/27/17.
 */

import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;

public class UlinkNormal {


    private String procCode = null;
    private String respCode = null;
    private String tranStat = null;



    public UlinkNormal() {
        this.procCode = ValueDefault.STRING_DEFAULT;
        this.respCode = ValueDefault.STRING_DEFAULT;
        this.tranStat = ValueDefault.STRING_DEFAULT;
    }

    public UlinkNormal(String procCode, String respCode, String tranStat) {
        this.procCode = procCode;
        this.respCode = respCode;
        this.tranStat = tranStat;
    }

    public void setProcCode(String procCode) {
        this.procCode = procCode;
    }

    public void setRespCode(String respCode) {
        this.respCode = respCode;
    }

    public void setTranStat(String tranStat) {
        this.tranStat = tranStat;
    }

    public String getProcCode() {
        return procCode;
    }

    public String getRespCode() {
        return respCode;
    }

    public String getTranStat() {
        return tranStat;
    }

}
