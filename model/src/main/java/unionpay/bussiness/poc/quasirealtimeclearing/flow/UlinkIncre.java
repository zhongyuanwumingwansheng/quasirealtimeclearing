package unionpay.bussiness.poc.quasirealtimeclearing.flow;

/**
 * Created by root on 7/27/17.
 */

import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;

public class UlinkIncre {
    private String transCdPay = null;
    private String paySt = null;
    private String transStRsvl = null;

    private String routInstIdCd = null;

    private String transSt = null;
    private String prodStyle = null;
    private int dRsvd6 = 0;


    public UlinkIncre() {
        this.transCdPay = ValueDefault.STRING_DEFAULT;
        this.paySt = ValueDefault.STRING_DEFAULT;
        this.transStRsvl = ValueDefault.STRING_DEFAULT;

        this.routInstIdCd = ValueDefault.STRING_DEFAULT;

        this.transSt = ValueDefault.STRING_DEFAULT;
        this.prodStyle = ValueDefault.STRING_DEFAULT;
        this.dRsvd6 = ValueDefault.INT_DEFAULT;
    }

    public UlinkIncre(String transCdPay, String paySt, String transStRsvl, String routInstIdCd, String transSt, String prodStyle, int dRsvd6) {
        this.transCdPay = transCdPay;
        this.paySt = paySt;
        this.transStRsvl = transStRsvl;
        this.routInstIdCd = routInstIdCd;
        this.transSt = transSt;
        this.prodStyle = prodStyle;
        this.dRsvd6 = dRsvd6;
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

    public int getdRsvd6() {
        return dRsvd6;
    }


}
