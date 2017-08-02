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

    /**
     * 交易金额,单位元
     */
    private double txnAmt = 0;

    /**
     * 手续费,单位分
     */
    private double exchange;

    public double getTxnAmt() {
        return txnAmt;
    }

    public void setTxnAmt(double txnAmt) {
        this.txnAmt = txnAmt;
    }


    public UlinkNormal() {
        super();
        this.procCode = ValueDefault.STRING_DEFAULT;
        this.respCode = ValueDefault.STRING_DEFAULT;
        this.tranStat = ValueDefault.STRING_DEFAULT;
        this.mId = ValueDefault.STRING_DEFAULT;
        this.txnAmt = ValueDefault.DOUBLE_DEFAULT;
        this.exchange =ValueDefault.DOUBLE_DEFAULT;
    }

    public UlinkNormal(String procCode, String respCode, String tranStat, String mId, double txnAmt, double exchange) {
        super();
        this.procCode = procCode;
        this.respCode = respCode;
        this.tranStat = tranStat;
        this.mId =mId;
        this.txnAmt =txnAmt;
        this.exchange=exchange;
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

    public void setExchange(double exchange) {
        this.exchange = exchange;
    }

    public double getExchange() {
        return exchange;
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
