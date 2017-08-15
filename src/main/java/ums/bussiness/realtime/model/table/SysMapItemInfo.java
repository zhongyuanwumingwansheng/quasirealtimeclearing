package ums.bussiness.realtime.model.table;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import ums.bussiness.realtime.common.ValueDefault;

import java.io.Serializable;

/**
 * Created by root on 8/1/17.
 */

public class SysMapItemInfo implements Serializable{

    @QuerySqlField(index = true)
    private String mapId;
    @QuerySqlField(index = true)
    private String srcItem;
    @QuerySqlField(index = false)
    private String mapResult;
    @QuerySqlField(index = true)
    private String typeId;

    public SysMapItemInfo(String mapId, String srcItem, String mapResult) {
        this.mapId = mapId;
        this.srcItem = srcItem;
        this.mapResult = mapResult;
    }

    public SysMapItemInfo() {
        this.mapId = ValueDefault.STRING_DEFAULT;
        this.srcItem = ValueDefault.STRING_DEFAULT;
        this.mapResult = ValueDefault.STRING_DEFAULT;
        this.typeId = ValueDefault.STRING_DEFAULT;
    }

    public String getMapId() {
        return mapId;
    }

    public String getSrcItem() {
        return srcItem;
    }

    public String getMapResult() {
        return mapResult;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setMapId(String mapId) {
        this.mapId = mapId;
    }

    public void setSrcItem(String srcItem) {
        this.srcItem = srcItem;
    }

    public void setMapResult(String mapResult) {
        this.mapResult = mapResult;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

}