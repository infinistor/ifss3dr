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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.util.List;

import com.pspace.ifs.DB.DRDBManager;
import com.pspace.ifs.DB.DRData;
import com.pspace.ifs.DB.UserData;
import com.pspace.ifs.DB.UserDBManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3DR {
	static final Logger logger = LoggerFactory.getLogger(S3DR.class);

	enum OperationList {
		createbucket, deletebucket, putobject, deleteobject, deletemarker, deletebucketwebsite, deletebucketpolicy, deletebucketcors,
		deletebucketlifecycle, deletebucketpab, deletebuckettagging, deletebucketencryption, deleteobjecttagging,
		putbucketacl, putbucketwebsite, putbucketpolicy, putbucketcors, putbucketlifecycle, putbucketversioning,
		putbucketpab, putbuckettagging, putbucketencryption, putobjectacl, putobjectretention, putobjecttagging, error
	}

	public static void main(String[] args) {

		//내 pid 저장
		if(!SavePid("/var/run/ifss-s3dr.pid"))
        {
            logger.error("Pid Save Failed!");
            return;
        }

		// 설정 읽기 
		String ConfingPath = "/usr/local/pspace/etc/ifss-s3dr.conf";
		S3DRConfig Config = new S3DRConfig(ConfingPath);
		if (!Config.GetConfig()) {
			logger.error("Config read failed!");
			return;
		}
		logger.debug("Config Read!");

		// DR DB 연결
		DRDBManager DRManager = new DRDBManager(Config.DBHost, Config.DBPort, Config.DRDBName, Config.DBUserName, Config.DBPassword);
		if (!DRManager.Connect()) {
			logger.error("DRDBManager connect failed!");
			return;
		}
		logger.debug("DRDBManager Connect!");

		// User DB 설정
		UserDBManager UserManager = new UserDBManager(Config.DBHost, Config.DBPort, Config.UserDBName, Config.DBUserName, Config.DBPassword);
		if (!UserManager.Connect()) {
			logger.error("UserDBManager connect failed!");
			return;
		}
		logger.debug("UserDbManager Connect!");

		UserManager.UpdateUserList();
		logger.debug("UpdateUserList End!");

		// S3 설정
		S3DRClient Client = new S3DRClient(Config.SourceURL, Config.TargetURL, Config.HttpPort, Config.HttpsPort);
		
		//Dump Check
		for (UserData User : UserManager.GetInitUserList()) {

			if(User.Dump) Client.SyncAll(User);
			else Client.SyncBucket(User);
			
			UserManager.UserInitOff(User);
		}

		long beforeTime = System.currentTimeMillis();
		while (true) {

			//User Update Check
			long afterTime = System.currentTimeMillis();
			if(TimeCheck(beforeTime, afterTime, Config.UpdateTime)){
				beforeTime = afterTime;
				UserManager.UpdateUserList();
				
				//Dump Check
				for (UserData User : UserManager.GetInitUserList()) {

					if(User.Dump) Client.SyncAll(User);
					else Client.SyncBucket(User);
					
					UserManager.UserInitOff(User);
				}
				UserManager.ClearInitUserList();
			}


			List<DRData> DRList = DRManager.GetDRList();

			if( DRList != null && DRList.size() > 0)
			{
				for (DRData Data : DRList) {

					//유저정보 가져오기
					UserData User = UserManager.GetUser(Data.Volume, Data.UserName);
	
					if (User == null) continue;
	
					//operation 
					OperationList menu = GetOperationToInt(Data.Operation);
	
					int RetryCount = 3;
					logger.debug(Data.toString());
					for(int i=0;i<RetryCount; i++)
					{
						boolean Result = false;
						switch(menu){
							case createbucket :
								Result = Client.PutBucket(Data.BucketName, User);
								break;
							case deletebucket :
								Result = Client.Deletebucket(Data.BucketName, User);
								break;
							case putobject :
								Result = Client.PutObject(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case deleteobject :
								Result = Client.DeleteObject(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case deletemarker :
								Result = Client.DeleteMarker(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case deletebucketwebsite :
								Result = Client.DeleteBucketWebsite(Data.BucketName, User);
								break;
							case deletebucketpolicy :
								Result = Client.DeleteBucketPolicy(Data.BucketName, User);
								break;
							case deletebucketcors :
								Result = Client.DeleteCORSConfiguration(Data.BucketName, User);
								break;
							case deletebucketlifecycle :
								Result = Client.DeleteLifecycleConfiguration(Data.BucketName, User);
								break;
							case deletebucketpab :
								Result = Client.DeletePublicAccessBlock(Data.BucketName, User);
								break;
							case deletebuckettagging :
								Result = Client.DeleteBucketTagging(Data.BucketName, User);
								break;
							case deletebucketencryption :
								Result = Client.DeleteBucketEncryption(Data.BucketName, User);
								break;
							case deleteobjecttagging :
								Result = Client.DeleteObjectTagging(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case putbucketacl :
								Result = Client.PutBucketACL(Data.BucketName, User);
								break;
							case putbucketwebsite :
								Result = Client.PutBucketWebsite(Data.BucketName, User);
								break;
							case putbucketpolicy :
								Result = Client.PutBucketPolicy(Data.BucketName, User);
								break;
							case putbucketcors :
								Result = Client.PutCORSConfiguration(Data.BucketName, User);
								break;
							case putbucketlifecycle :
								Result = Client.PutLifecycleConfiguration(Data.BucketName, User);
								break;
							case putbucketversioning :
								Result = Client.PutBucketVersioning(Data.BucketName, User);
								break;
							case putbucketpab :
								Result = Client.PutPublicAccessBlock(Data.BucketName, User);
								break;
							case putbuckettagging :
								Result = Client.PutBucketTagging(Data.BucketName, User);
								break;
							case putbucketencryption :
								Result = Client.PutBucketEncryption(Data.BucketName, User);
								break;
							case putobjectacl :
								Result = Client.PutObjectACL(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case putobjectretention :
								Result = Client.PutObjectRetention(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case putobjecttagging :
								Result = Client.PutObjectTagging(Data.BucketName, Data.ObjectName, Data.VersionId, User);
								break;
							case error :
								logger.error("operation does not match");
								break;
						}
						if (Result) break;
					}
				}

				int LastID = DRList.get(DRList.size() - 1).ID;
				DRManager.DeleteListIndex(LastID);
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("Sleep ERROR : ", "", e);
			}
		}
	}
	
	public static boolean SavePid(String FilePath)
	{
		try {
			String temp = ManagementFactory.getRuntimeMXBean().getName();
			int index = temp.indexOf("@");
			String PID = temp.substring(0, index);
	
			File file = new File(FilePath);
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			writer.write(PID);
			writer.close();
			return true;
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
	}

	public static OperationList GetOperationToInt(String Operation){
		for (OperationList Item : OperationList.values()) {
			if(Operation.equals(Item.toString())) return Item;
		}
		return OperationList.error;
	}

	public static boolean TimeCheck(long before, long after, int delay){
		long DiffTime = (after - before) / 60000; //1min

		if (DiffTime > delay) return true;
		return false;		
	}
}
