package com.localblox.ashfaq.config;

/**
 * Config properties file which instantiates only once.
 */
public class AppConfig {

    private final String s3AccessKeyId;
    private final String s3SecretAccessKey;
    private final String s3Password;

    private static volatile AppConfig instance;

    private AppConfig(String s3AccessKeyId, String s3SecretAccessKey, String s3Password) {
        this.s3AccessKeyId = s3AccessKeyId;
        this.s3SecretAccessKey = s3SecretAccessKey;
        this.s3Password = s3Password;
    }

    public static void initConfig(String s3AccessKeyId, String s3SecretAccessKey, String s3Password) {
        AppConfig localInstance = instance;
        if (localInstance == null) {
            synchronized (AppConfig.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = new AppConfig(s3AccessKeyId, s3SecretAccessKey, s3Password);
                }
            }
        }
    }

    public static AppConfig getInstance() {
        if (instance == null) {
            throw new IllegalStateException("");
        }
        return instance;
    }

    public String getS3AccessKeyId() {
        return s3AccessKeyId;
    }

    public String getS3SecretAccessKey() {
        return s3SecretAccessKey;
    }

    public String getS3Password() {
        return s3Password;
    }
}
