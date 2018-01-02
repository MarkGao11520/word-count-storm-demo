package com.gwf.trident.event;

import java.io.Serializable;

public class DiagnosisEvent implements Serializable{
    private static final long serialVersionUID = -8503836414563089938L;

    private double lat;
    private double lng;
    private long time;
    private String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        super();
        this.lat = lat;
        this.lng = lng;
        this.time = time;
        this.diagnosisCode = diagnosisCode;
    }

    public double getLat() {
        return lat;
    }

    public double getLng() {
        return lng;
    }

    public long getTime() {
        return time;
    }

    public String getDiagnosisCode() {
        return diagnosisCode;
    }
}
