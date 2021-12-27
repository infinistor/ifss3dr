/*
* Copyright 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE.md for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package com.pspace.ifs.S3DR;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3DRConfig {
	static final Logger logger = LoggerFactory.getLogger(S3DRConfig.class);
    /////////////////////////////////// Global /////////////////////////////////////////
    private final String STR_GLOBAL = "Global";
    private final String STR_SOURCE_URL = "source_url";
    private final String STR_TARGET_URL = "target_url";
    private final String STR_HTTP_PORT = "http_port";
    private final String STR_HTTPS_PORT = "https_port";
    ///////////////////////////////////// DB ///////////////////////////////////////////
    private final String STR_DB             = "DB";
    private final String STR_DB_HOST        = "host";
    private final String STR_DB_PORT        = "port";
    private final String STR_DB_DR_DBNAME   = "dr_dbname";
    private final String STR_DB_USER_DBNAME = "user_dbname";
    private final String STR_DB_USER        = "username";
    private final String STR_DB_PASSWORD    = "password";
    private final String STR_DB_UPDATE_TIME = "update_time";

    /*********************************************************************************************************/
    public final String FileName;
    private final Ini ini = new Ini();
    /*********************************************************************************************************/
    public String SourceURL;
    public String TargetURL;
    public int HttpPort;
    public int HttpsPort;

    public String DBHost;
    public int    DBPort;
    public String DRDBName;
    public String UserDBName;
    public String DBUserName;
    public String DBPassword;
    public int    UpdateTime;

    public S3DRConfig(String FileName) {
        this.FileName = FileName;
    }

    public boolean GetConfig() {
        try {
            if (FileName.isEmpty()) return false;
            
            File file = new File(FileName);
            ini.load(new FileReader(file));

            //Global
            SourceURL = ReadKeyToString(STR_GLOBAL, STR_SOURCE_URL);
            TargetURL = ReadKeyToString(STR_GLOBAL, STR_TARGET_URL);
            HttpPort  = ReadKeyToInt(STR_GLOBAL, STR_HTTP_PORT);
            HttpsPort = ReadKeyToInt(STR_GLOBAL, STR_HTTPS_PORT);

            //DB
            DBHost     = ReadKeyToString(STR_DB, STR_DB_HOST);
            DBPort     = ReadKeyToInt(STR_DB, STR_DB_PORT);
            DRDBName   = ReadKeyToString(STR_DB, STR_DB_DR_DBNAME);
            UserDBName = ReadKeyToString(STR_DB, STR_DB_USER_DBNAME);
            DBUserName = ReadKeyToString(STR_DB, STR_DB_USER);
            DBPassword = ReadKeyToString(STR_DB, STR_DB_PASSWORD);
            UpdateTime = ReadKeyToInt(STR_DB, STR_DB_UPDATE_TIME);

        } catch (InvalidFileFormatException e) {
            logger.error("FileName : {}", FileName, e);
            return false;
        } catch (FileNotFoundException e) {
            logger.error("FileName : {}", FileName, e);
            return false;
        } catch (IOException e) {
            logger.error("FileName : {}", FileName, e);
            return false;
        }
        return true;
    }

    @Override
    public String toString(){
        return String.format(
        "S3ReplicatorConfig{\n" + 
            "\t%s{\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%s,\n" + 
            "\t},\n" + 
            "\t%s{\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%d,\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%s,\n" + 
                "\t\t%s:%d\n" + 
            "\t}\n" + 
        "}",
            STR_GLOBAL,
                STR_SOURCE_URL, SourceURL,
                STR_TARGET_URL, TargetURL,
            STR_DB,
                STR_DB_HOST, DBHost,
                STR_DB_PORT, DBPort,
                STR_DB_DR_DBNAME, DRDBName,
                STR_DB_USER_DBNAME, UserDBName,
                STR_DB_USER, DBUserName,
                STR_DB_PASSWORD, DBPassword,
                STR_DB_UPDATE_TIME, UpdateTime);
    }

    private String ReadKeyToString(String Section, String Key) {
        return ini.get(Section, Key);
    }

    private int ReadKeyToInt(String Section, String Key) {
        return Integer.parseInt(ini.get(Section, Key));
    }

    // private boolean ReadKeyToBoolean(String Section, String Key) {
    //     return Boolean.parseBoolean(ini.get(Section, Key));
    // }
}
