package unionpay.bussiness.poc.quasirealtimeclearing;

/**
 * Created by root on 7/27/17.
 */

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import unionpay.bussiness.poc.quasirealtimeclearing.flow.UlinkIncre;
import unionpay.bussiness.poc.quasirealtimeclearing.flow.UlinkNormal;
import unionpay.bussiness.poc.quasirealtimeclearing.flow.Ulink;

import java.util.List;
import java.util.ArrayList;

public class PretreatmentTest {

    private KieServices ks = null;
    private KieContainer kContainer = null;
    private KieSession kSession = null;

    @Before
    public void initKie() {
        ks = KieServices.Factory.get();
        kContainer = ks.getKieClasspathContainer();
        kSession = kContainer.newKieSession();
    }


    @Test
    public void testUlinkincreFilter() {
        UlinkIncre ui1 = new UlinkIncre("02S221X1", "s1", "s2", "s3", "s4", "s5", 1, "");
        UlinkIncre ui2 = new UlinkIncre("02V523X1", "s1", "s2", "s3", "s4", "s5", 1, "");
        UlinkIncre ui3 = new UlinkIncre("07S30609", "0", "0", "3748020000", "0", "51A2", 1001, "");
        UlinkIncre ui4 = new UlinkIncre("s0", "s1", "s2", "s3", "s4", "s5", 1, "");
        UlinkIncre ui5 = new UlinkIncre("07S30609", "0", "1", "3748020000", "0", "51A2", 1001, "");
        kSession.insert(ui1);
        kSession.insert(ui2);
        kSession.insert(ui3);
        kSession.insert(ui4);
        kSession.insert(ui5);
        int fired = kSession.fireAllRules();
        long reserved = kSession.getFactCount();
        assert (fired == 3);
        assert (reserved == 2l);
    }

    @Test
    public void testUlinknormalFilter() {
        UlinkNormal un1 = new UlinkNormal("03", "00", "1", "");
        UlinkNormal un2 = new UlinkNormal("01", "XY", "1", "");
        UlinkNormal un3 = new UlinkNormal("02", "00", "4", "");
        UlinkNormal un4 = new UlinkNormal("01", "10", "3", "");
        UlinkNormal un5 = new UlinkNormal("02", "Y1", "2", "");
        kSession.insert(un1);
        kSession.insert(un2);
        kSession.insert(un3);
        kSession.insert(un4);
        kSession.insert(un5);
        int fired = kSession.fireAllRules();
        long reserved = kSession.getFactCount();
        assert (fired == 3);
        assert (reserved == 2l);
    }

/*    @Test
    public void tempTest() {
        Ulink u= new Ulink();
        System.out.println(u.getClearingFlag());
        List<String> list= new ArrayList<String>(){{
            add("2");
            add("3");
            add("4");
            add("5");
            add("6");
        }};
        kSession.setGlobal("list", list);
        kSession.insert(u);
        kSession.fireAllRules();

    }*/

    @After
    public void deposeKie() {
        ks = null;
        kContainer = null;
        kSession = null;
    }
}
