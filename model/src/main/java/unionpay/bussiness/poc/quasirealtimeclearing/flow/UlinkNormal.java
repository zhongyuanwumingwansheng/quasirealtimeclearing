package unionpay.bussiness.poc.quasirealtimeclearing.flow;

/**
 * Created by root on 7/27/17.
 */

import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;

public class UlinkNormal extends Ulink{

    /**
     * 交易处理码
     */
    private String procCode = null;
    /**
     * 交易应答码
     */
    private String respCode = null;
    /**
     * 交易应答状态
     */
    private String tranStat = null;
    /**
     * 支付商户号
     */
    private String mId = null;


    public UlinkNormal() {
        super();
        this.procCode = ValueDefault.STRING_DEFAULT;
        this.respCode = ValueDefault.STRING_DEFAULT;
        this.tranStat = ValueDefault.STRING_DEFAULT;
        this.mId = ValueDefault.STRING_DEFAULT;
    }

    public UlinkNormal(String procCode, String respCode, String tranStat, String mId) {
        super();
        this.procCode = procCode;
        this.respCode = respCode;
        this.tranStat = tranStat;
        this.mId =mId;
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

    public void setmId(String mId) {
        this.mId = mId;
    }

    public String getmId() {
        return mId;
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
