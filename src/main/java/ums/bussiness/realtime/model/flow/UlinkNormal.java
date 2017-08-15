package ums.bussiness.realtime.model.flow;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 7/27/17.
 */


public class UlinkNormal extends Ulink implements Serializable {

    /**
     * 交易处理码
     */
    @QuerySqlField(index = true)
    private String procCode = null;
    /**
     * 交易应答码
     */
    @QuerySqlField(index = false)
    private String respCode = null;
    /**
     * 交易应答状态
     */
    @QuerySqlField(index = false)
    private String tranStat = null;

    /**
     * 支付商户号
     */
    @QuerySqlField(index = true)
    private String mId = null;
    /**
     * 终端id
     */
    @QuerySqlField(index = true)
    private String tId = null;
    /**
     * 无
     */
    @QuerySqlField(index = false)
    private String serConcode = null;


    /**
     * 消息类型
     */
    @QuerySqlField(index = false)
    private String msgType = null;


    /**
     * 筛选标志位
     */
    private boolean filterFlag;

    /**
     * 交易金额,单位元
     */
    @QuerySqlField(index = false)
    private double txnAmt = 0;

    /**
     * 手续费,单位分
     */
    @QuerySqlField(index = false)
    private double exchange;

    /**
     * REV4
     */
    @QuerySqlField(index = false)
    private String RSV4;

    public UlinkNormal() {
        super();
        this.procCode = ValueDefault.STRING_DEFAULT;
        this.respCode = ValueDefault.STRING_DEFAULT;
        this.tranStat = ValueDefault.STRING_DEFAULT;
        this.mId = ValueDefault.STRING_DEFAULT;
        this.tId = ValueDefault.STRING_DEFAULT;
        this.msgType = ValueDefault.STRING_DEFAULT;
        this.serConcode = ValueDefault.STRING_DEFAULT;
        this.filterFlag = ValueDefault.BOOLEAN_DEFAULT;
        this.txnAmt = ValueDefault.DOUBLE_DEFAULT;
        this.exchange = ValueDefault.DOUBLE_DEFAULT;
        this.RSV4 = ValueDefault.STRING_DEFAULT;
    }

    public UlinkNormal(String procCode, String respCode, String tranStat, String mId, String tId, String serConcode, String msgType, boolean filterFlag, double txnAmt, double exchange) {
        super();
        this.procCode = procCode;
        this.respCode = respCode;
        this.tranStat = tranStat;
        this.mId = mId;
        this.tId = tId;
        this.serConcode = serConcode;
        this.msgType = msgType;
        this.filterFlag = filterFlag;
        this.txnAmt = txnAmt;
        this.exchange = exchange;
    }

    public String toString(){
        return "UlinkNormal: " + "|procCode:" + procCode + "|respCode:" + respCode + "|tranStat:" + tranStat + "|mId:" + mId + "|tId:" + tId + "|serConcode:" + serConcode + "|msgType:" + msgType + "|filterFlag:" + filterFlag + "|txnAmt:" + txnAmt + "|exchange:" + exchange;
    }
    public String getProcCode() {
        return procCode;
    }

    public void setProcCode(String procCode) {
        this.procCode = procCode;
    }

    public String getRespCode() {
        return respCode;
    }

    public void setRespCode(String respCode) {
        this.respCode = respCode;
    }

    public String getTranStat() {
        return tranStat;
    }

    public void setTranStat(String tranStat) {
        this.tranStat = tranStat;
    }

    public String getmId() {
        return mId;
    }

    public void setmId(String mId) {
        this.mId = mId;
    }

    public String gettId() {
        return tId;
    }

    public void settId(String tId) {
        this.tId = tId;
    }

    public String getSerConcode() {
        return serConcode;
    }

    public void setSerConcode(String serConcode) {
        this.serConcode = serConcode;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public boolean isFilterFlag() {
        return filterFlag;
    }

    public void setFilterFlag(boolean filterFlag) {
        this.filterFlag = filterFlag;
    }

    public double getTxnAmt() {
        return txnAmt;
    }

    public void setTxnAmt(double txnAmt) {
        this.txnAmt = txnAmt;
    }

    public double getExchange() {
        return exchange;
    }

    public void setExchange(double exchange) {
        this.exchange = exchange;
    }

    public String getRSV4() {
        return RSV4;
    }

    public void setRSV4(String RSV4) {
        this.RSV4 = RSV4;
    }
}
