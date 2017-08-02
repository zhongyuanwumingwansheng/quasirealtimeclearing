package unionpay.bussiness.poc.quasirealtimeclearing.flow;

/**
 * Created by root on 7/27/17.
 */
import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;

public class UlinkIncre extends Ulink {
    /**
     * 支付交易代码
     */
    private String transCdPay = null;
    /**
     * 支付交易状态
     */
    private String paySt = null;
    /**
     * 交易冲正状态
     */
    private String transStRsvl = null;
    /**
     * 支付方路由机构代码
     */
    private String routInstIdCd = null;
    /**
     * 交易应答状态
     */
    private String transSt = null;
    /**
     * 产品类型
     */
    private String prodStyle = null;
    /**
     * 请求保留
     */
    private int dRsvd6 = 0;
    /**
     * 支付商户号
     */
    private String mchntIdPay = null;
    /**
     * 扩展属性, 清算标志，借贷标志，分组id，商户号
     */
    private String settleFlag = null;
    private String dcFlag = null;
    private String groupId = null;
    private String merNo = null;

    /**
     * 筛选标志位
     */
    private Boolean filterFlag = Boolean.FALSE;

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
        this.settleFlag = ValueDefault.STRING_DEFAULT;
        this.dcFlag = ValueDefault.STRING_DEFAULT;
        this.groupId = ValueDefault.STRING_DEFAULT;
        this.merNo = ValueDefault.STRING_DEFAULT;
    }

    public UlinkIncre(String transCdPay, String paySt, String transStRsvl, String routInstIdCd, String transSt,
                      String prodStyle, int dRsvd6, String mchntIdPay) {

        super();
        this.transCdPay = transCdPay;
        this.paySt = paySt;
        this.transStRsvl = transStRsvl;
        this.routInstIdCd = routInstIdCd;
        this.transSt = transSt;
        this.prodStyle = prodStyle;
        this.dRsvd6 = dRsvd6;
        this.mchntIdPay = mchntIdPay;
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

    public void setdRsvd6(int dRsvd6) {
        this.dRsvd6 = dRsvd6;
    }

    public void setMchntIdPay(String mchntIdPay) {
        this.mchntIdPay = mchntIdPay;
    }

    public void setSettleFlag(String settleFlag) {
        this.settleFlag = settleFlag;
    }

    public void setdcFlag(String dcFlag) {
        this.dcFlag = dcFlag;
    }

    public void setGroupId(String groupId) {this.groupId = groupId; }

    public void setMerNo(String merNo) {this.merNo = merNo;}

    public void setFilterFlag(Boolean filterFlag) {this.filterFlag = filterFlag; }

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

    public String getSettleFlag() { return settleFlag; }

    public String getdcFlag() { return dcFlag; }

    public String getGroupId(){return groupId; }

    public String getMerNo(){return merNo; }

    public int getdRsvd6() {
        return dRsvd6;
    }

    public int getNum(int number){
        return number;
    }

    public Boolean getFilterFlag() {return filterFlag; }


}
