package com.samhad.spark.common;

import org.apache.spark.sql.SparkSession;

public class InitializerVO {

    private final SparkSession session;
    private final String packageName;

    public InitializerVO(SparkSession session, String packageName) {
        this.session = session;
        this.packageName = packageName;
    }

    public SparkSession getSession() {
        return session;
    }

    public String getPackageName() {
        return packageName;
    }
}
