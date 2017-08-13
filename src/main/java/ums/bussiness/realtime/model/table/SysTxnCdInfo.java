package ums.bussiness.realtime.model.table;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 7/27/17.
 */


public class SysTxnCdInfo implements Serializable{
/*    字段名称	类型	关键字	允许为NULL	说明	备注
    txn_key	varchar(50)	PK	NOT NULL	代码索引	FILE_IMP_INFO.KEY_LIST
    txn_code	char(3)		NOT NULL	交易代码
    txn_des	varchar(50)		NOT NULL	交易描述
    bms_txn_code	cha4(4)		NOT NULL	BMS交易代码
    sett_flg	char(1)		NOT NULL	清算标志	0：不清算
					1：正常交易，清算
					2：正常撤销交易，清算
					3：正常交易，清算
					4：正常交易，清算
					5：差错交易，清算
					6：差错交易，清算
					7：特殊退单交易，不清算
					8：特殊退单交易，不清算
    E：找不到扣率信息，不清算
    F：退货超限额挂帐交易，不清算
    H：中间业务对帐不平，长款，不清算
    N：与银联流水对帐不平，长款，不清算
    Z：银商咨询流水，不清算
    W：回传当地
    dc_flg	integer		NOT NULL	借贷标记	1: 借记
					-1： 贷记
    txn_code_grp	char(2)		NOT NULL	交易类型分类标识
    rcd_ver	integer		NOT NULL	记录版本
    add_datetime	char(14)		NOT NULL	新增记录时间
    add_user_id	char(10)		NOT NULL	新增记录操作员
    upd_datetime	char(14)			修改记录时间
    upd_user_id	char(10)			修改记录操作员*/
    /**
     * 代码索引
     */
    @QuerySqlField(index = true)
    private String txnKey;
    /**
     * 交易代码
     */
    @QuerySqlField(index = true)
    private String txnCode;
    /**
     * 交易描述
     */
    @QuerySqlField(index = true)
    private String txnDes;
    /**
     * BMS交易代码
     */
    @QuerySqlField(index = true)
    private String bmsTxnCode;
    /**
     * 清算标志
     * 0：不清算
     * 1：正常交易，清算
     * 2：正常撤销交易，清算
     * 3：正常交易，清算
     * 4：正常交易，清算
     * 5：差错交易，清算
     * 6：差错交易，清算
     * 7：特殊退单交易，不清算
     * 8：特殊退单交易，不清算
     * E：找不到扣率信息，不清算
     * F：退货超限额挂帐交易，不清算
     * H：中间业务对帐不平，长款，不清算
     * N：与银联流水对帐不平，长款，不清算
     * Z：银商咨询流水，不清算
     * W：回传当地

     */
    @QuerySqlField(index = true)
    private String settFlg;
    /**
     * 借贷标记
     * 1: 借记
     * -1： 贷记
     */
    @QuerySqlField(index = true)
    private int dcFlg;
    /**
     * 交易类型分类标识
     */
    @QuerySqlField(index = true)
    private String txnCodeGrp;
    /**
     * 记录版本
     */
    @QuerySqlField(index = true)
    private int rcdVer;
    /**
     * 新增记录时间
     */
    @QuerySqlField(index = true)
    private String addDatetime;
    /**
     * 新增记录操作员
     */
    @QuerySqlField(index = true)
    private String addUserId;
    /**
     * 修改记录时间
     */
    @QuerySqlField(index = true)
    private String updDatetime;
    /**
     * 修改记录操作员
     */
    @QuerySqlField(index = true)
    private String updUserId;

    public SysTxnCdInfo() {
        this.txnKey = ValueDefault.STRING_DEFAULT;
        this.txnCode = ValueDefault.STRING_DEFAULT;
        this.txnDes = ValueDefault.STRING_DEFAULT;
        this.bmsTxnCode = ValueDefault.STRING_DEFAULT;
        this.settFlg = ValueDefault.STRING_DEFAULT;
        this.dcFlg = ValueDefault.INT_DEFAULT;
        this.txnCodeGrp = ValueDefault.STRING_DEFAULT;
        this.rcdVer = ValueDefault.INT_DEFAULT;
        this.addDatetime = ValueDefault.STRING_DEFAULT;
        this.addUserId = ValueDefault.STRING_DEFAULT;
        this.updDatetime = ValueDefault.STRING_DEFAULT;
        this.updUserId = ValueDefault.STRING_DEFAULT;
    }

    public SysTxnCdInfo(String txnKey, String txnCode, String txnDes, String bmsTxnCode, String settFlg, int dcFlg, String txnCodeGrp, int rcdVer, String addDatetime, String addUserId, String updDatetime, String updUserId) {
        this.txnKey = txnKey;
        this.txnCode = txnCode;
        this.txnDes = txnDes;
        this.bmsTxnCode = bmsTxnCode;
        this.settFlg = settFlg;
        this.dcFlg = dcFlg;
        this.txnCodeGrp = txnCodeGrp;
        this.rcdVer = rcdVer;
        this.addDatetime = addDatetime;
        this.addUserId = addUserId;
        this.updDatetime = updDatetime;
        this.updUserId = updUserId;
    }

    public String getTxnKey() {
        return txnKey;
    }

    public String getTxnCode() {
        return txnCode;
    }

    public String getTxnDes() {
        return txnDes;
    }

    public String getBmsTxnCode() {
        return bmsTxnCode;
    }

    public String getSettFlg() {
        return settFlg;
    }

    public int getDcFlg() {
        return dcFlg;
    }

    public String getTxnCodeGrp() {
        return txnCodeGrp;
    }

    public int getRcdVer() {
        return rcdVer;
    }

    public String getAddDatetime() {
        return addDatetime;
    }

    public String getAddUserId() {
        return addUserId;
    }

    public String getUpdDatetime() {
        return updDatetime;
    }

    public String getUpdUserId() {
        return updUserId;
    }

    public void setTxnKey(String txnKey) {
        this.txnKey = txnKey;
    }

    public void setTxnCode(String txnCode) {
        this.txnCode = txnCode;
    }

    public void setTxnDes(String txnDes) {
        this.txnDes = txnDes;
    }

    public void setBmsTxnCode(String bmsTxnCode) {
        this.bmsTxnCode = bmsTxnCode;
    }

    public void setSettFlg(String settFlg) {
        this.settFlg = settFlg;
    }

    public void setDcFlg(int dcFlg) {
        this.dcFlg = dcFlg;
    }

    public void setTxnCodeGrp(String txnCodeGrp) {
        this.txnCodeGrp = txnCodeGrp;
    }

    public void setRcdVer(int rcdVer) {
        this.rcdVer = rcdVer;
    }

    public void setAddDatetime(String addDatetime) {
        this.addDatetime = addDatetime;
    }

    public void setAddUserId(String addUserId) {
        this.addUserId = addUserId;
    }

    public void setUpdDatetime(String updDatetime) {
        this.updDatetime = updDatetime;
    }

    public void setUpdUserId(String updUserId) {
        this.updUserId = updUserId;
    }
}
