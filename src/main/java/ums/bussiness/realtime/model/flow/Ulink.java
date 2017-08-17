package ums.bussiness.realtime.model.flow;


import ums.bussiness.realtime.common.ValueDefault;

import java.util.Date;

/**
 * Created by root on 7/31/17.
 */

public class Ulink {
    /**
     * 借贷标记
     * 1: 借记
     * -1： 贷记
     */
    private int dcFlag;
    /**
     * 清算标志
     */
    private String clearingFlag;
    /**
     * 商户ID
     */
    private int merId;
    /**
     * 分组ID
     */
    private String groupId;
    /**
     * 商户编号
     */
    private String merNo;

    /**
     * 流水时间
     */
    private Date uTime;
    /**
     * 无商户档案标志, false 表示有商户档案
     */
    private boolean noBmsStlInfo;
    /**
     * 支持的商户计费类型"10"||"11", false 表示不是支持的这两种计费类型
     */
    private boolean supportedCreditCalcType;


    public int getDcFlag() {
        return dcFlag;
    }

    public String getClearingFlag() {
        return clearingFlag;
    }

    public int getMerId() {
        return merId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getMerNo() {
        return merNo;
    }

    public Date getuTime() {
        return uTime;
    }

    public boolean getNoBmsStlInfo() {return noBmsStlInfo; }

    public void setNoBmsStlInfo(boolean noBmsStlInfo) {this.noBmsStlInfo = noBmsStlInfo; }

    public boolean getSupportedCreditCalcType() {return supportedCreditCalcType; }

    public void setSupportedCreditCalcType(boolean supportedCreditCalcType) {this.supportedCreditCalcType = supportedCreditCalcType; }

    public void setDcFlag(int dcFlag) {
        this.dcFlag = dcFlag;
    }

    public void setClearingFlag(String clearingFlag) {
        this.clearingFlag = clearingFlag;
    }

    public void setMerId(int merId) {
        this.merId = merId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setMerNo(String merNo) {
        this.merNo = merNo;
    }

    public void setuTime(Date uTime) {
        this.uTime = uTime;
    }
    //TODO,eliminate Hard Code
    public Ulink() {
        this.dcFlag = ValueDefault.INT_DEFAULT;
        this.clearingFlag = ValueDefault.STRING_DEFAULT;
        this.merId = ValueDefault.INT_DEFAULT;
        this.groupId = ValueDefault.STRING_DEFAULT;
        this.merNo = ValueDefault.STRING_DEFAULT;
        this.uTime = new Date();
        this.noBmsStlInfo = ValueDefault.BOOLEAN_DEFAULT;
        this.supportedCreditCalcType = ValueDefault.BOOLEAN_DEFAULT;
    }

    public Ulink(int dcFlag, String clearingFlag, int merId, String groupId, String merNo, Date uTime) {
        this.dcFlag = dcFlag;
        this.clearingFlag = clearingFlag;
        this.merId = merId;
        this.groupId = groupId;
        this.merNo = merNo;
        this.uTime = uTime;
    }
}
