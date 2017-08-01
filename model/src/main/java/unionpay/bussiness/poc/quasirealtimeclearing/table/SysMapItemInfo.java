package unionpay.bussiness.poc.quasirealtimeclearing.table;

/**
 * Created by root on 8/1/17.
 */
import unionpay.bussiness.poc.quasirealtimeclearing.ValueDefault;
public class SysMapItemInfo {
    public String getMapId() {
        return mapId;
    }

    public String getSrcItem() {
        return srcItem;
    }

    public String getMapResult() {
        return mapResult;
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

    public SysMapItemInfo(String mapId, String srcItem, String mapResult) {
        this.mapId = mapId;
        this.srcItem = srcItem;
        this.mapResult = mapResult;
    }

    public SysMapItemInfo() {
        this.mapId = ValueDefault.STRING_DEFAULT;
        this.srcItem = ValueDefault.STRING_DEFAULT;
        this.mapResult = ValueDefault.STRING_DEFAULT;
    }

    private String mapId=null;
    private String srcItem=null;
    private String mapResult=null;
}
