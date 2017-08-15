package ums.bussiness.realtime.model.flow;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 7/27/17.
 */

public class UlinkIncre extends Ulink implements Serializable{
    /**
     * 支付交易代码
     */
    @QuerySqlField(index = true)
    private String transCdPay;
    /**
     * 支付交易状态
     */
    @QuerySqlField(index = true)
    private String paySt;
    /**
     * 交易冲正状态
     */
    @QuerySqlField(index = true)
    private String transStRsvl;
    /**
     * 支付方路由机构代码
     */
    @QuerySqlField(index = true)
    private String routInstIdCd;
    /**
     * 交易应答状态
     */
    @QuerySqlField(index = true)
    private String transSt;
    /**
     * 产品类型
     */
    @QuerySqlField(index = true)
    private String prodStyle;
    /**
     * 请求保留
     */
    @QuerySqlField(index = true)
    private String dRsvd1;
    /**
     * 请求保留
     */
    @QuerySqlField(index = true)
    private int dRsvd6;
    /**
     * 支付商户号
     */
    @QuerySqlField(index = true)
    private String mchntIdPay;

    public String getTermIdPay() {
        return termIdPay;
    }

    public void setTermIdPay(String termIdPay) {
        this.termIdPay = termIdPay;
    }

    /**
     * 支付终端号
     */
    private String termIdPay;

    public double getExchange() {
        return exchange;
    }

    public void setExchange(double exchange) {
        this.exchange = exchange;
    }

    /**
     * 筛选标志位

     */
    private boolean filterFlag;

    /**
     * 交易金额,单位分
     */
    private double transAmt;

    /**
     * 手续费,单位分
     */
    private double exchange;

    public UlinkIncre() {
        super();
        this.transCdPay = ValueDefault.STRING_DEFAULT;
        this.paySt = ValueDefault.STRING_DEFAULT;
        this.transStRsvl = ValueDefault.STRING_DEFAULT;
        this.routInstIdCd = ValueDefault.STRING_DEFAULT;
        this.transSt = ValueDefault.STRING_DEFAULT;
        this.prodStyle = ValueDefault.STRING_DEFAULT;
        this.dRsvd6 = ValueDefault.INT_DEFAULT;
        this.mchntIdPay = ValueDefault.STRING_DEFAULT;
        this.termIdPay = ValueDefault.STRING_DEFAULT;
        this.filterFlag = ValueDefault.BOOLEAN_DEFAULT;
        this.transAmt = ValueDefault.DOUBLE_DEFAULT;
        this.exchange = ValueDefault.DOUBLE_DEFAULT;
    }

    public UlinkIncre(String transCdPay, String paySt, String transStRsvl, String routInstIdCd, String transSt, String prodStyle, int dRsvd6, String mchntIdPay, String termIdPay, boolean filterFlag, double transAmt, double exchange) {
        super();
        this.transCdPay = transCdPay;
        this.paySt = paySt;
        this.transStRsvl = transStRsvl;
        this.routInstIdCd = routInstIdCd;
        this.transSt = transSt;
        this.prodStyle = prodStyle;
        this.dRsvd6 = dRsvd6;
        this.mchntIdPay = mchntIdPay;
        this.termIdPay = termIdPay;
        this.filterFlag = filterFlag;
        this.transAmt = transAmt;
        this.exchange = exchange;
    }

    public void setTransCdPay(String transCdPay) {
        this.transCdPay = transCdPay;
    }

    public void setPaySt(String paySt) {
        this.paySt = paySt;
    }

    public void setTransStRsvl(String transStRsvl) {
        this.transStRsvl = transStRsvl;
    }

    public void setRoutInstIdCd(String routInstIdCd) {
        this.routInstIdCd = routInstIdCd;
    }

    public void setTransSt(String transSt) {
        this.transSt = transSt;
    }

    public void setProdStyle(String prodStyle) {
        this.prodStyle = prodStyle;
    }

    public void setdRsvd1(String dRsvd1) {
        this.dRsvd1 = dRsvd1;
    }

    public void setdRsvd6(int dRsvd6) {
        this.dRsvd6 = dRsvd6;
    }

    public void setMchntIdPay(String mchntIdPay) {
        this.mchntIdPay = mchntIdPay;
    }

    public void setFilterFlag(boolean filterFlag) {this.filterFlag = filterFlag; }

    public void setTransAmt(double transAmt) {
        this.transAmt = transAmt;
    }

    public double getTransAmt() {
        return transAmt;
    }

    public String getMchntIdPay() {
        return mchntIdPay;
    }

    public String getTransCdPay() {
        return transCdPay;
    }

    public String getPaySt() {
        return paySt;
    }

    public String getTransStRsvl() {
        return transStRsvl;
    }

    public String getRoutInstIdCd() {
        return routInstIdCd;
    }

    public String getTransSt() {
        return transSt;
    }

    public String getProdStyle() {
        return prodStyle;
    }

    public String getdRsvd1() {
        return dRsvd1;
    }

    public int getdRsvd6() {
        return dRsvd6;
    }

    public boolean getFilterFlag() {return filterFlag; }


}
