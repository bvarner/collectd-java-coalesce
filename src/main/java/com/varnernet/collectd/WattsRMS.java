package com.varnernet.collectd;

import org.collectd.api.Collectd;
import org.collectd.api.CollectdReadInterface;
import org.collectd.api.CollectdTargetFactoryInterface;
import org.collectd.api.CollectdTargetInterface;
import org.collectd.api.DataSet;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A non-generic (highly specific) class for aggregating lots of small milli-volt DC readings into
 * AC RMS. Useful for measuring CT clamps through burdens.
 *
 * @author bryan at varnernet.com
 */
public class WattsRMS implements CollectdTargetFactoryInterface, CollectdReadInterface {

    public static final String PLUGIN_TARGET_NAME = "Watts-RMS";

    private ArrayList<RMSTarget> targets = new ArrayList<>();

    /**
     * Public constructor invoked by <code>LoadPlugin</code> config callback.
     */
    public WattsRMS() {
        Collectd.registerTarget(PLUGIN_TARGET_NAME, this);
        Collectd.registerRead(PLUGIN_TARGET_NAME, this);
    }

    @Override
    public CollectdTargetInterface createTarget(OConfigItem ci) {
        if (!ci.getKey().equalsIgnoreCase("Target") &&
                ci.getValues().size() != 1 &&
                !ci.getValues().get(0).getString().equalsIgnoreCase("PLUGIN_TARGET_NAME")) {
            Collectd.logError("Coalesce asked to create non-coalesce target: " + ci);
            return null;
        }

        double burden = 0;
        double vac = 120;
        for (OConfigItem child : ci.getChildren()) {
            if (child.getKey().equalsIgnoreCase("Burden")) {
                burden = child.getValues().get(0).getNumber().doubleValue();
            }
            if (child.getKey().equalsIgnoreCase("ACVolts")) {
                vac = child.getValues().get(0).getNumber().doubleValue();
            }
        }

        RMSTarget target = new RMSTarget(burden, vac);
        targets.add(target);
        return target;
    }


    @Override
    public int read() {
        long when = System.currentTimeMillis();
        for (int i = 0; i < targets.size(); i++) {
            List<ValueList> results = targets.get(i).read(when);
            for (int j = 0; j < results.size(); j++) {
                Collectd.dispatchValues(results.get(j));
            }
        }
        return 0;
    }

    /**
     * A Target instance.
     */
    private class RMSTarget implements CollectdTargetInterface {
        private double burden;
        private double vac;
        private Map<String, ValueList> peaks = new ConcurrentHashMap<>(4, 1, 5);


        private RMSTarget(final double burden, final double vac) {
            this.burden = burden;
            this.vac = vac;
        }

        /**
         * Since the invoke method can invoke for more than one value list / data set combo (we can get multiple things
         * sent to us to be coalesced / aggregated, make sure we maintain multiple 'window's for the histogram data.
         *
         * @param ds
         * @param vl
         * @return
         */
        @Override
        public int invoke(DataSet ds, ValueList vl) {
            String key = vl.getSource();
            Double currentValue = vl.getValues().get(0).doubleValue();
            ValueList currentPeak = peaks.get(key);
            if (currentPeak == null) {
                Collectd.logDebug("Registering newly targeted source: " + key);
                peaks.put(key, vl);
            } else if (currentPeak.getValues().get(0).doubleValue() < currentValue) {
                peaks.put(key, vl);
            }
            return Collectd.FC_TARGET_CONTINUE;
        }

        /**
         * Return a ValueList for every 'window' we have.
         *
         * @param when
         * @return
         */
        private List<ValueList> read(long when) {
            List<ValueList> list = new ArrayList<>(peaks.size());

            for (Map.Entry<String, ValueList> peak : peaks.entrySet()) {
                list.add(calculate(when, peak.getValue()));
                peak.getValue().getValues().set(0, Double.MIN_VALUE);
            }

            return list;
        }

        /**
         * @param when
         * @param peakValue
         * @return
         */
        private ValueList calculate(long when, ValueList peakValue) {
            // Create a copy of the first data set in the thing.
            ValueList ret = new ValueList(peakValue);
            ret.setInterval(0l);
            ret.setTime(when);
            ret.setTypeInstance(ret.getTypeInstance() + "-" + PLUGIN_TARGET_NAME);

            // Start with the peak value since the last read cycle.
            double peak = peakValue.getValues().get(0).doubleValue();

            // If we have a burden, convert voltage to milliamps.
            if (burden != 0) {
                peak /= burden;
            }

            // Scale to Amperes.
            peak *= 1000.0d;

            // divide by sqrt(2) (or multiply by... 0.7071 for the lazy).
            peak *= 0.7071d;

            // Multiply by voltage....
            peak *= vac;

            // And now we have Watts-RMS over the 'window' view. ;-)
            ret.getValues().set(0, peak);

            return ret;
        }
    }
}
