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
package com.pspace.ifs.DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.pspace.ifs.S3DR.S3DR;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRDBManager {

	private final String TABLE_NAME 	= "s3drlist";
	private final String STR_ID 		= "id";
	private final String STR_VOLUME 	= "volume";
	private final String STR_USERNAME 	= "username";
	private final String STR_OPERATION 	= "operation";
	private final String STR_BUCKETNAME = "bucketname";
	private final String STR_OBJECTNAME = "objectname";
	private final String STR_VERSIONID 	= "versionid";

	private final String SelectAllDataQuery = "SELECT * FROM " + TABLE_NAME + " LIMIT 1000;";
	private final String DeleteLastIndexQuery = "Delete from " + TABLE_NAME + " where ID < ?;";
	private final String CREATE_S3_REPLICATOR_LIST = 
								"CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ( " +
								STR_ID + " bigint auto_increment primary key, " +
								STR_VOLUME + " varchar(128) not null, " +
								STR_USERNAME + " varchar(128) not null, " +
								STR_OPERATION + " varchar(64) not null, " +
								STR_BUCKETNAME + " varchar(256) not null, " +
								STR_OBJECTNAME + " varchar(2048) null, " +
								STR_VERSIONID + " varchar(32) null, " + 
								"timestamp datetime DEFAULT NULL);";
	/****************************************************************************/
    private PreparedStatement pstSelectAllData;
	private PreparedStatement pstDeleteLastIndex;
	/****************************************************************************/
	static final Logger logger = LoggerFactory.getLogger(S3DR.class);
    
    private final String Host;
    private final int Port;
    private final String DataBaseName;
    private final String UserName;
    private final String Password;

    private Connection conn = null;

	public DRDBManager(String Host, int Port, String DataBaseName, String UserName, String Password){
		this.Host = Host;
		this.Port = Port;
		this.DataBaseName = DataBaseName;
		this.UserName = UserName;
		this.Password = Password;
	}
	
	public boolean Connect()
	{
        String URL = String.format("jdbc:mysql://%s:%d/%s?useSSL=false", Host, Port, DataBaseName);

		try{
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(URL, UserName, Password);
			CreateTable();
			pstSelectAllData = conn.prepareStatement(SelectAllDataQuery);
			pstDeleteLastIndex = conn.prepareStatement(DeleteLastIndexQuery);
			return true;
		}
		catch(Exception e){
			logger.error("URL : {}", URL, e);
			return false;
		}
	}

	private boolean CreateTable()
	{
		try(Statement stmt = conn.createStatement()){
			stmt.executeUpdate(CREATE_S3_REPLICATOR_LIST);
			stmt.close();
			return true;
		}catch(Exception e){
			logger.error("", e);
			return false;
		}
	}

	public List<DRData> GetDRList()
	{
		List<DRData> DRList = new ArrayList<DRData>();
		try (ResultSet rs = pstSelectAllData.executeQuery()){
			
			while(rs.next())
			{
				DRList.add(new DRData(
					rs.getInt   (STR_ID),
					rs.getString(STR_VOLUME),
					rs.getString(STR_USERNAME),
					rs.getString(STR_OPERATION),
					rs.getString(STR_BUCKETNAME),
					rs.getString(STR_OBJECTNAME),
					rs.getString(STR_VERSIONID)
				));
			}
		} catch (SQLException e) {
			logger.error("", e);
		}

		return DRList;
	}
	
	public Boolean DeleteListIndex(int index)
	{
		try{
			pstDeleteLastIndex.clearParameters();
			pstDeleteLastIndex.setInt(1, index + 1);
			pstDeleteLastIndex.executeUpdate();
			return true;
		} catch (SQLException e) {
			logger.error("", e);
			return false;
		}
	}
}
