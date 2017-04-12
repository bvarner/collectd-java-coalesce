package com.varnernet.collectd;

import org.collectd.api.Collectd;
import org.collectd.api.CollectdTargetFactoryInterface;
import org.collectd.api.CollectdTargetInterface;
import org.collectd.api.DataSet;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

/**
 * A base class for creating targets with (complex?) computational semantics far beyond the 'aggregation'
 * plugin for collectd.
 *
 * @author bryan at varnernet.com
 */
public class Coalesce implements CollectdTargetFactoryInterface, CollectdTargetInterface {

    public Coalesce() {
        Collectd.registerTarget("coalesce", this);
        Collectd.logInfo("Registered target: coalesce");
    }

    @Override
    public CollectdTargetInterface createTarget(OConfigItem ci) {
        Collectd.logInfo("createTarget: " + ci);
        return this;
    }


    @Override
    public int invoke(DataSet ds, ValueList vl) {
        Collectd.logInfo("Target invoked with: " + ds + " and " + vl);
        return Collectd.FC_TARGET_CONTINUE;
    }

}
