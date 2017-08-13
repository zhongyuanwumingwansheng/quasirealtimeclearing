package ums.bussiness.realtime.model.table;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 7/27/17.
 */

public class BmsStInfo implements Serializable{

    /**
     * 商户ID
     */
    @QuerySqlField(index = true)
    private String merId;
    /**
     * 商户编号
     */
    @QuerySqlField(index = true)
    private String merNo;
/*    //Maybe use later
    private String batDate=null;
    private String instId=null;*/
    /**
     * 主应用
     */
    @QuerySqlField(index = true)
    private String mappMain;
    /**
     * 多应用类型
     */
    @QuerySqlField(index = false)
    private int apptypeId;
    /**
     * 计费方式
     * 00 - 不收费
     * 10 - 固定比例收费
     * 11 - 固定金额收费
     * 其他类型待处理
     */
    @QuerySqlField(index = false)
    private String creditCalcType;
    /**
     * 费率
     */
    @QuerySqlField(index = false)
    private double creditCalcRate;
    /**
     * 手续费
     */
    @QuerySqlField(index = false)
    private double creditCalcAmt;
    /**
     * 最低收费金额
     */
    @QuerySqlField(index = false)
    private double creditMinAmt;
    /**
     * 最高收费金额
     */
    @QuerySqlField(index = false)
    private double creditMaxAmt;

    public BmsStInfo() {
        this.merId = ValueDefault.STRING_DEFAULT;
        this.merNo = ValueDefault.STRING_DEFAULT;
        this.mappMain = ValueDefault.STRING_DEFAULT;
        this.apptypeId = ValueDefault.INT_DEFAULT;
        this.creditCalcType = ValueDefault.STRING_DEFAULT;
        this.creditCalcRate = ValueDefault.DOUBLE_DEFAULT;
        this.creditCalcAmt = ValueDefault.DOUBLE_DEFAULT;
        this.creditMinAmt = ValueDefault.DOUBLE_DEFAULT;
        this.creditMaxAmt = ValueDefault.DOUBLE_DEFAULT;
    }

    public BmsStInfo(String merId, String merNo, String mappMain, int apptypeId, String creditCalcType, double creditCalcRate, double creditCalcAmt, double creditMinAmt, double creditMaxAmt) {
        this.merId = merId;
        this.merNo = merNo;
        this.mappMain = mappMain;
        this.apptypeId = apptypeId;
        this.creditCalcType = creditCalcType;
        this.creditCalcRate = creditCalcRate;
        this.creditCalcAmt = creditCalcAmt;
        this.creditMinAmt = creditMinAmt;
        this.creditMaxAmt = creditMaxAmt;
    }

    public String getMerId() {
        return merId;
    }

    public String getMerNo() {
        return merNo;
    }

    public String getMappMain() {
        return mappMain;
    }

    public int getApptypeId() {
        return apptypeId;
    }

    public String getCreditCalcType() {
        return creditCalcType;
    }

    public double getCreditCalcRate() {
        return creditCalcRate;
    }

    public double getCreditCalcAmt() {
        return creditCalcAmt;
    }

    public double getCreditMinAmt() {
        return creditMinAmt;
    }

    public double getCreditMaxAmt() {
        return creditMaxAmt;
    }

    public void setMerId(String merId) {
        this.merId = merId;
    }

    public void setMerNo(String merNo) {
        this.merNo = merNo;
    }

    public void setMappMain(String mappMain) {
        this.mappMain = mappMain;
    }

    public void setApptypeId(int apptypeId) {
        this.apptypeId = apptypeId;
    }

    public void setCreditCalcType(String creditCalcType) {
        this.creditCalcType = creditCalcType;
    }

    public void setCreditCalcRate(double creditCalcRate) {
        this.creditCalcRate = creditCalcRate;
    }

    public void setCreditCalcAmt(double creditCalcAmt) {
        this.creditCalcAmt = creditCalcAmt;
    }

    public void setCreditMinAmt(double creditMinAmt) {
        this.creditMinAmt = creditMinAmt;
    }

    public void setCreditMaxAmt(double creditMaxAmt) {
        this.creditMaxAmt = creditMaxAmt;
    }
}
