package ums.bussiness.realtime.model.table;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 7/27/17.
 */

/*inst_id	char(8)	PK	NOT NULL	银商机构号
        bat_date	char(8)	PK	NOT NULL	批量日期
        group_id	varchar(8)	PK	NOT NULL	分组ID
        group_type_id	varchar(8)	PK	NOT NULL	分组类型ID
        item	varchar(2000)	PK	NOT NULL	分组成员
        become_effective_date	char(8)		NOT NULL	生效日期	默认99991231
        lost_effective_date	char(8)		NOT NULL	失效日期
        rcd_ver	integer		NOT NULL	记录版本
        add_datetime	char(14)		NOT NULL	新增记录时间
        add_user_id	char(10)		NOT NULL	新增记录操作员
        upd_datetime	char(14)			修改记录时间
        upd_user_id	char(10)			修改记录操作员*/

public class SysGroupItemInfo implements Serializable{
    /**
     * 银商机构号
     */
    @QuerySqlField(index = false)
    private String instId;
    /**
     * 批量日期
     */
    @QuerySqlField(index = false)
    private String batDate;
    /**
     * 分组ID
     */
    @QuerySqlField(index = true)
    private String groupId;
    /**
     * 分组类型ID
     */
    @QuerySqlField(index = true)
    private String groupTypeId;
    /**
     * 分组成员
     */
    @QuerySqlField(index = true)
    private String item;
    /**
     * 生效日期
     */
    @QuerySqlField(index = false)
    private String becomeEffectiveDate;
    /**
     * 失效日期
     */
    @QuerySqlField(index = false)
    private String lostEffectiveDate;
    /**
     * 记录版本
     */
    @QuerySqlField(index = false)
    private int rcdVer;
    /**
     * 新增记录时间
     */
    @QuerySqlField(index = false)
    private String addDatetime;
    /**
     * 新增记录操作员
     */
    @QuerySqlField(index = false)
    private String addUserId;
    /**
     * 修改记录时间
     */
    @QuerySqlField(index = false)
    private String updDatetime;
    /**
     * 修改记录操作员
     */
    @QuerySqlField(index = false)
    private String updUserId;

    public SysGroupItemInfo() {
        this.instId = ValueDefault.STRING_DEFAULT;
        this.batDate = ValueDefault.STRING_DEFAULT;
        this.groupId = ValueDefault.STRING_DEFAULT;
        this.groupTypeId = ValueDefault.STRING_DEFAULT;
        this.item = ValueDefault.STRING_DEFAULT;
        this.becomeEffectiveDate = ValueDefault.STRING_DEFAULT;
        this.lostEffectiveDate = ValueDefault.STRING_DEFAULT;
        this.rcdVer = ValueDefault.INT_DEFAULT;
        this.addDatetime = ValueDefault.STRING_DEFAULT;
        this.addUserId = ValueDefault.STRING_DEFAULT;
        this.updDatetime = ValueDefault.STRING_DEFAULT;
        this.updUserId = ValueDefault.STRING_DEFAULT;
    }

    public SysGroupItemInfo(String instId, String batDate, String groupId, String groupTypeId, String item, String becomeEffectiveDate, String lostEffectiveDate, int rcdVer, String addDatetime, String addUserId, String updDatetime, String updUserId) {
        this.instId = instId;
        this.batDate = batDate;
        this.groupId = groupId;
        this.groupTypeId = groupTypeId;
        this.item = item;
        this.becomeEffectiveDate = becomeEffectiveDate;
        this.lostEffectiveDate = lostEffectiveDate;
        this.rcdVer = rcdVer;
        this.addDatetime = addDatetime;
        this.addUserId = addUserId;
        this.updDatetime = updDatetime;
        this.updUserId = updUserId;
    }

    public String getInstId() {
        return instId;
    }

    public String getBatDate() {
        return batDate;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getGroupTypeId() {
        return groupTypeId;
    }

    public String getItem() {
        return item;
    }

    public String getBecomeEffectiveDate() {
        return becomeEffectiveDate;
    }

    public String getLostEffectiveDate() {
        return lostEffectiveDate;
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

    public void setInstId(String instId) {
        this.instId = instId;
    }

    public void setBatDate(String batDate) {
        this.batDate = batDate;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setGroupTypeId(String groupTypeId) {
        this.groupTypeId = groupTypeId;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public void setBecomeEffectiveDate(String becomeEffectiveDate) {
        this.becomeEffectiveDate = becomeEffectiveDate;
    }

    public void setLostEffectiveDate(String lostEffectiveDate) {
        this.lostEffectiveDate = lostEffectiveDate;
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
