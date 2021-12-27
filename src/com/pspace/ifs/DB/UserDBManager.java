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
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserDBManager {
    static final Logger logger = LoggerFactory.getLogger(UserDBManager.class);
    /*****************************************************************************************/
	private final String TABLE_NAME        = "S3DrUserListTable";
	private final String STR_VOLUME        = "volume";
	private final String STR_USERNAME      = "username";
	private final String STR_ACCESS_KEY    = "access_key";
	private final String STR_ACCESS_SECRET = "access_secret";
	private final String STR_DUMP          = "dump";
	private final String STR_INIT          = "init";

	private final String CREATE_S3_DR_USER_LIST_TABLE = 
								"CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ( " +
								STR_VOLUME + " varchar(700) not null, " +
								STR_USERNAME + " varchar(700) not null, " +
								STR_ACCESS_KEY + " varchar(40) not null, " +
								STR_ACCESS_SECRET + " bool not null, " +
								STR_DUMP + " varchar(2048) null, " +
                                STR_INIT + " bool default true, " +
								"primary key(volume, username));";
	private final String SelectAllDataQuery  = "SELECT * FROM " + TABLE_NAME + " LIMIT 1000;";
	private final String UpdateUserDataQuery = "UPDATE " + TABLE_NAME + " SET init=false WHERE " + STR_VOLUME + "=? and " + STR_USERNAME + "=?";
    /*****************************************************************************************/
    private PreparedStatement pstSelectAllUserData;
    private PreparedStatement pstUpdateUserData;
    /*****************************************************************************************/
    private final String Host;
    private final int    Port;
    private final String DBName;
    private final String UserName;
    private final String Password;
    
    private Connection conn = null;

    private HashMap<String, UserData> Users;
	private List<UserData> InitUsers;

    public UserDBManager(String Host, int Port, String DBName, String UserName, String Password)
    {
        this.Host = Host;
		this.Port = Port;
		this.DBName = DBName;
		this.UserName = UserName;
		this.Password = Password;
        Users = new HashMap<String, UserData>();
		InitUsers = new ArrayList<UserData>();
    }

    public boolean Connect()
    {
        String URL = String.format("jdbc:mysql://%s:%d/%s?useSSL=false", Host, Port, DBName);

		try{
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(URL, UserName, Password);
			CreateTable();
			pstSelectAllUserData = conn.prepareStatement(SelectAllDataQuery);
            pstUpdateUserData = conn.prepareStatement(UpdateUserDataQuery);
			return true;
		}
		catch(Exception e){
			logger.error("URL : {}", URL, e);
			return false;
		}
    }
    
	private boolean CreateTable()
	{
		try{
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(CREATE_S3_DR_USER_LIST_TABLE);
			stmt.close();
			return true;
		}catch(Exception e){
			logger.error("CreateTable Failed:", e);
			return false;
		}
	}

    public boolean UpdateUserList()
    {
        Users.clear();
		InitUsers.clear();
		try {
			ResultSet rs = pstSelectAllUserData.executeQuery();
            
			while(rs.next())
			{
                UserData User = new UserData(
					rs.getString(STR_VOLUME),
					rs.getString(STR_USERNAME),
					rs.getString(STR_ACCESS_KEY),
					rs.getString(STR_ACCESS_SECRET),
					rs.getBoolean(STR_DUMP)
                    );

				boolean Init = rs.getBoolean(STR_INIT);

				Users.put(GetKeyName(User.Volume, User.Name), User);
				if(Init) InitUsers.add(User);
			}
            return true;
		} catch (SQLException e) {
			logger.error("", e);
            return false;
		}
    }

    public UserData GetUser(String Volume, String UserName)
    {
        return Users.get(GetKeyName(Volume, UserName));
    }

	public List<UserData> GetInitUserList(){
		return InitUsers;
	}
	public void ClearInitUserList(){
		InitUsers.clear();
	}


    public boolean UserInitOff(UserData User)
    {
        try{
            pstUpdateUserData.clearParameters();
            pstUpdateUserData.setString(1, User.Volume);
            pstUpdateUserData.setString(2, User.Name);
            pstUpdateUserData.executeUpdate();
            return true;
        }
        catch(Exception e)
        {
			logger.error("", e);
			return false;
        }
    }
    /***************************************************************************************************/
    public static String GetKeyName(String Volume, String UserName)
    {
        return Volume + UserName;
    }
}
