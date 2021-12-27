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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.pspace.ifs.DB.UserData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3DRClient {
    static final Logger logger = LoggerFactory.getLogger(S3DRClient.class);

    static final String HEADER_VERSIONID = "x-amz-ifs-VersionId";
    static final String HEADER_NO_DR = "x-amz-ifs-nodr";
    static final String HEADER_DR_GET = "x-amz-ifs-s3dr-get";
    static final String HEADER_SSE_C = "x-amz-ifs-s3dr-sse-key";
    static final String HEADER_SSE_C_KEY = "x-amz-server-side-encryption-customer-key";
    static final String HEADER_ENCRYPTION_SIZE = "x-amz-ifs-encryption-size";
    static final String HEADER_ENCRYPTION_ETAG = "x-amz-ifs-encryption-etag";


    public final String SourceURL;
    public final String TargetURL;
    public final int HttpPort;
    public final int HttpsPort;
    public AmazonS3 Client = null;

    public S3DRClient(String SourceURL, String TargetURL, int HttpPort, int HttpsPort) {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

        this.SourceURL = SourceURL;
        this.TargetURL = TargetURL;
        this.HttpPort = HttpPort;
        this.HttpsPort = HttpsPort;
    }

    /******************************************
     * SYNC API
     **************************************************/
    public boolean SyncAll(UserData User) {
        String data = String.format(User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);

            List<Bucket> BucketList = SourceClient.listBuckets();
            List<Thread> ThreadList = new ArrayList<Thread>();
            for (Bucket bucket : BucketList) {

                // Bucket Sync
                String BucketName = bucket.getName();
                PutBucket(BucketName, User);
                PutBucketVersioning(BucketName, User);
                PutBucketACL(BucketName, User);
                PutCORSConfiguration(BucketName, User);
                PutBucketTagging(BucketName, User);
                PutLifecycleConfiguration(BucketName, User);
                PutBucketPolicy(BucketName, User);
                PutBucketEncryption(BucketName, User);
                PutBucketWebsite(BucketName, User);

                // Object Sync
                Thread MyThread = new Thread(() -> SyncObject(BucketName, User));
                MyThread.start();
                ThreadList.add(MyThread);
            }

            for (Thread thread : ThreadList) {
                thread.join();
            }

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean SyncBucket(UserData User) {
        String data = String.format(User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);

            List<Bucket> BucketList = SourceClient.listBuckets();

            for (Bucket bucket : BucketList) {

                // Bucket Sync
                String BucketName = bucket.getName();
                PutBucket(BucketName, User);
                PutBucketVersioning(BucketName, User);
                PutBucketACL(BucketName, User);
                PutCORSConfiguration(BucketName, User);
                PutBucketTagging(BucketName, User);
                PutLifecycleConfiguration(BucketName, User);
                PutBucketPolicy(BucketName, User);
                PutBucketEncryption(BucketName, User);
                PutBucketWebsite(BucketName, User);
            }

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public void SyncObject(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            VersionListing Response = SourceClient.listVersions(BucketName, "");

            while (true) {
                List<S3VersionSummary> ObjectList = Response.getVersionSummaries();
                Collections.reverse(ObjectList);

                for (S3VersionSummary object : ObjectList) {
                    String ObjectName = object.getKey();
                    String VersionId = object.getVersionId();
                    PutObject(BucketName, ObjectName, VersionId, User);
                    PutObjectRetention(BucketName, ObjectName, VersionId, User);
                }
                if (Response.isTruncated())
                    Response = SourceClient.listVersions(BucketName, "");
                else
                    break;
            }

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
    }

    /*****************************************
     * Bucket API
     *************************************************/
    public boolean PutBucket(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            // 버킷 생성
            CreateBucketRequest createBucketRequest = new CreateBucketRequest(BucketName);
            createBucketRequest.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.createBucket(createBucketRequest);

            // 버킷 ACL 정보 입력
            AccessControlList ACL = SourceClient.getBucketAcl(BucketName);
            SetBucketAclRequest setBucketAclRequest = new SetBucketAclRequest(BucketName, ACL);
            setBucketAclRequest.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketAcl(BucketName, ACL);

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean Deletebucket(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketRequest Request = new DeleteBucketRequest(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucket(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketVersioning(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketVersioningConfiguration VersioningConfiguration = SourceClient
                    .getBucketVersioningConfiguration(BucketName);
            SetBucketVersioningConfigurationRequest Request = new SetBucketVersioningConfigurationRequest(BucketName,
                    VersioningConfiguration);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketVersioningConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketACL(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            AccessControlList ACL = SourceClient.getBucketAcl(BucketName);
            SetBucketAclRequest Request = new SetBucketAclRequest(BucketName, ACL);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketAcl(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutCORSConfiguration(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketCrossOriginConfiguration CORSConfiguration = SourceClient
                    .getBucketCrossOriginConfiguration(BucketName);
            SetBucketCrossOriginConfigurationRequest Request = new SetBucketCrossOriginConfigurationRequest(BucketName,
                    CORSConfiguration);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketCrossOriginConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteCORSConfiguration(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketCrossOriginConfigurationRequest Request = new DeleteBucketCrossOriginConfigurationRequest(
                    BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketCrossOriginConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketTagging(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketTaggingConfiguration Tagging = SourceClient.getBucketTaggingConfiguration(BucketName);
            SetBucketTaggingConfigurationRequest Request = new SetBucketTaggingConfigurationRequest(BucketName,
                    Tagging);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketTaggingConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteBucketTagging(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketTaggingConfigurationRequest Request = new DeleteBucketTaggingConfigurationRequest(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketTaggingConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutLifecycleConfiguration(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketLifecycleConfiguration LifecycleConfiguration = SourceClient
                    .getBucketLifecycleConfiguration(BucketName);
            SetBucketLifecycleConfigurationRequest Request = new SetBucketLifecycleConfigurationRequest(BucketName,
                    LifecycleConfiguration);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketLifecycleConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteLifecycleConfiguration(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketLifecycleConfigurationRequest Request = new DeleteBucketLifecycleConfigurationRequest(
                    BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketLifecycleConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketPolicy(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketPolicy Policy = SourceClient.getBucketPolicy(BucketName);
            SetBucketPolicyRequest Request = new SetBucketPolicyRequest(BucketName, Policy.getPolicyText());
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketPolicy(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteBucketPolicy(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketPolicyRequest Request = new DeleteBucketPolicyRequest(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketPolicy(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutObjectLockConfiguration(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            GetObjectLockConfigurationResult ObjectLockConfiguration = SourceClient
                    .getObjectLockConfiguration(new GetObjectLockConfigurationRequest().withBucketName(BucketName));
            SetObjectLockConfigurationRequest Request = new SetObjectLockConfigurationRequest()
                    .withBucketName(BucketName)
                    .withObjectLockConfiguration(ObjectLockConfiguration.getObjectLockConfiguration());
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setObjectLockConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutPublicAccessBlock(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            GetPublicAccessBlockResult PublicAccessBlock = SourceClient
                    .getPublicAccessBlock(new GetPublicAccessBlockRequest().withBucketName(BucketName));
            SetPublicAccessBlockRequest Request = new SetPublicAccessBlockRequest().withBucketName(BucketName)
                    .withPublicAccessBlockConfiguration(PublicAccessBlock.getPublicAccessBlockConfiguration());
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setPublicAccessBlock(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeletePublicAccessBlock(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeletePublicAccessBlockRequest Request = new DeletePublicAccessBlockRequest().withBucketName(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deletePublicAccessBlock(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketEncryption(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            GetBucketEncryptionResult BucketEncryption = SourceClient.getBucketEncryption(BucketName);
            SetBucketEncryptionRequest Request = new SetBucketEncryptionRequest().withBucketName(BucketName)
                    .withServerSideEncryptionConfiguration(BucketEncryption.getServerSideEncryptionConfiguration());
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketEncryption(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteBucketEncryption(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketEncryptionRequest Request = new DeleteBucketEncryptionRequest().withBucketName(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketEncryption(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutBucketWebsite(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            BucketWebsiteConfiguration BucketWebsite = SourceClient.getBucketWebsiteConfiguration(BucketName);
            SetBucketWebsiteConfigurationRequest Request = new SetBucketWebsiteConfigurationRequest(BucketName,
                    BucketWebsite);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.setBucketWebsiteConfiguration(Request);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteBucketWebsite(String BucketName, UserData User) {
        String data = String.format("{BucketName : %s, UserData : %s", BucketName, User.toString());
        logger.debug("Start : {}", data);

        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            DeleteBucketWebsiteConfigurationRequest Request = new DeleteBucketWebsiteConfigurationRequest(BucketName);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteBucketWebsiteConfiguration(Request);

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    /*****************************************
     * Object API
     *************************************************/
    public boolean PutObject(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName, ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            InputStream Body = null;
            ObjectMetadata Metadata = null;

            if (FolderCheck(ObjectName)) {
                GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(BucketName, ObjectName);
                getObjectMetadataRequest.putCustomRequestHeader(HEADER_DR_GET, "none"); // Header add
                if (VersionId != null && !VersionId.isEmpty()) getObjectMetadataRequest.setVersionId(VersionId);
                
                Metadata = SourceClient.getObjectMetadata(getObjectMetadataRequest);
                Body = CreateBody("");
            } else {

                GetObjectRequest getObjectRequest = new GetObjectRequest(BucketName, ObjectName);
                if (VersionId != null && !VersionId.isEmpty()) getObjectRequest.setVersionId(VersionId);
                getObjectRequest.putCustomRequestHeader(HEADER_DR_GET, "none"); // Header add
                S3Object ObjectData = SourceClient.getObject(getObjectRequest);

                Body = ObjectData.getObjectContent();
                Metadata = ObjectData.getObjectMetadata();
            }


            GetObjectAclRequest getObjectAclRequest = new GetObjectAclRequest(BucketName, ObjectName);
            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BucketName, ObjectName);

            if (VersionId != null && !VersionId.isEmpty()) {
                getObjectAclRequest.setVersionId(VersionId);
                getObjectTaggingRequest.setVersionId(VersionId);
            }

            AccessControlList ACL = SourceClient.getObjectAcl(getObjectAclRequest);
            GetObjectTaggingResult Tagging = SourceClient.getObjectTagging(getObjectTaggingRequest);

            Metadata.setHeader(HEADER_VERSIONID, VersionId);
            Metadata.setHeader(HEADER_NO_DR, "none");

            //SSEC CHECK
            Object SSEKey = Metadata.getRawMetadataValue(HEADER_SSE_C_KEY);
            if (SSEKey != null)
            {
                Metadata.setHeader(HEADER_ENCRYPTION_ETAG, Metadata.getETag());
                Metadata.setHeader(HEADER_SSE_C, SSEKey.toString());
            }

            PutObjectRequest putObjectRequest = new PutObjectRequest(BucketName, ObjectName, Body, Metadata);

            putObjectRequest.setAccessControlList(ACL);
            putObjectRequest.setTagging(new ObjectTagging(Tagging.getTagSet()));

            TargetClient.putObject(putObjectRequest);
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteObject(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            if (VersionId == null || VersionId.isEmpty()) {
                DeleteObjectRequest Request = new DeleteObjectRequest(BucketName, ObjectName);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.deleteObject(Request);
            } else {
                DeleteVersionRequest Request = new DeleteVersionRequest(BucketName, ObjectName, VersionId);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.deleteVersion(Request);
            }
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteMarker(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            // GeneratePresignedUrlRequest Request = new
            // GeneratePresignedUrlRequest(BucketName, ObjectName, HttpMethod.DELETE);
            // Request.setExpiration(GetTimeToAddSeconds(100000));
            // Request.putCustomRequestHeader(HEADER_VERSIONID, VersionId);
            // Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            // URL url = TargetClient.generatePresignedUrl(Request);
            // HttpResponse Response = DeleteObjectURL(url, VersionId);
            // if ( Response.getStatusLine().getStatusCode() != 204){
            // logger.error("{}", data);
            // }

            DeleteObjectRequest Request = new DeleteObjectRequest(BucketName, ObjectName);
            Request.putCustomRequestHeader(HEADER_VERSIONID, VersionId);
            Request.putCustomRequestHeader(HEADER_NO_DR, "none");
            TargetClient.deleteObject(Request);

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutObjectACL(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            if (VersionId == null || VersionId.isEmpty()) {
                AccessControlList ACL = SourceClient.getObjectAcl(BucketName, ObjectName);
                SetObjectAclRequest Request = new SetObjectAclRequest(BucketName, ObjectName, ACL);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectAcl(Request);
            } else {
                AccessControlList ACL = SourceClient.getObjectAcl(BucketName, ObjectName, VersionId);
                SetObjectAclRequest Request = new SetObjectAclRequest(BucketName, ObjectName, VersionId, ACL);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectAcl(Request);
            }

            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutObjectTagging(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            if (VersionId == null || VersionId.isEmpty()) {
                GetObjectTaggingResult Tagging = SourceClient
                        .getObjectTagging(new GetObjectTaggingRequest(BucketName, ObjectName));
                ObjectTagging ObjectTagging = new ObjectTagging(Tagging.getTagSet());
                SetObjectTaggingRequest Request = new SetObjectTaggingRequest(BucketName, ObjectName, ObjectTagging);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectTagging(Request);
            } else {
                GetObjectTaggingResult Tagging = SourceClient
                        .getObjectTagging(new GetObjectTaggingRequest(BucketName, ObjectName, VersionId));
                ObjectTagging ObjectTagging = new ObjectTagging(Tagging.getTagSet());
                SetObjectTaggingRequest Request = new SetObjectTaggingRequest(BucketName, ObjectName, VersionId,
                        ObjectTagging);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectTagging(Request);
            }
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean DeleteObjectTagging(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 TargetClient = CreateTargetClient(User);
            if (VersionId == null || VersionId.isEmpty()) {
                DeleteObjectTaggingRequest Request = new DeleteObjectTaggingRequest(BucketName, ObjectName);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.deleteObjectTagging(Request);
            } else {
                DeleteObjectTaggingRequest Request = new DeleteObjectTaggingRequest(BucketName, ObjectName)
                        .withVersionId(VersionId);
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.deleteObjectTagging(Request);
            }
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutObjectRetention(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            if (VersionId == null || VersionId.isEmpty()) {
                GetObjectRetentionResult ObjectRetention = SourceClient.getObjectRetention(
                        new GetObjectRetentionRequest().withBucketName(BucketName).withKey(ObjectName));
                SetObjectRetentionRequest Request = new SetObjectRetentionRequest().withBucketName(BucketName)
                        .withKey(ObjectName).withRetention(ObjectRetention.getRetention());
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectRetention(Request);
            } else {
                GetObjectRetentionResult ObjectRetention = SourceClient
                        .getObjectRetention(new GetObjectRetentionRequest().withBucketName(BucketName)
                                .withKey(ObjectName).withVersionId(VersionId));
                SetObjectRetentionRequest Request = new SetObjectRetentionRequest().withBucketName(BucketName)
                        .withKey(ObjectName).withVersionId(VersionId).withRetention(ObjectRetention.getRetention());
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectRetention(Request);
            }
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    public boolean PutObjectLegalHold(String BucketName, String ObjectName, String VersionId, UserData User) {
        String data = String.format("{BucketName : %s, ObjectName : %s, VersionID : %s, UserData : %s", BucketName,
                ObjectName, VersionId, User.toString());
        logger.debug("Start : {}", data);
        try {
            AmazonS3 SourceClient = CreateSourceClient(User);
            AmazonS3 TargetClient = CreateTargetClient(User);

            if (VersionId == null || VersionId.isEmpty()) {
                GetObjectLegalHoldResult ObjectLegalHold = SourceClient.getObjectLegalHold(
                        new GetObjectLegalHoldRequest().withBucketName(BucketName).withKey(ObjectName));
                SetObjectLegalHoldRequest Request = new SetObjectLegalHoldRequest().withBucketName(BucketName)
                        .withKey(ObjectName).withLegalHold(ObjectLegalHold.getLegalHold());
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectLegalHold(Request);
            } else {
                GetObjectLegalHoldResult ObjectLegalHold = SourceClient
                        .getObjectLegalHold(new GetObjectLegalHoldRequest().withBucketName(BucketName)
                                .withKey(ObjectName).withVersionId(VersionId));
                SetObjectLegalHoldRequest Request = new SetObjectLegalHoldRequest().withBucketName(BucketName)
                        .withKey(ObjectName).withVersionId(VersionId).withLegalHold(ObjectLegalHold.getLegalHold());
                Request.putCustomRequestHeader(HEADER_NO_DR, "none");
                TargetClient.setObjectLegalHold(Request);
            }
            return true;

        } catch (Exception e) {
            logger.error("{}\n", data, e);
        }
        return false;
    }

    /************************************************
     * ETC
     ****************************************************/
    public static String CreateURL(String Address, int Port, boolean IsSecure) {
        if (IsSecure)
            return String.format("https://%s:%d", Address, Port);
        else
            return String.format("http://%s:%d", Address, Port);
    }

    public static AmazonS3 CreateClient(String URL, UserData User) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(User.AccessKey, User.SecretKey);

        return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, ""))
                .withPathStyleAccessEnabled(true).build();
    }

    public AmazonS3 CreateSourceClient(UserData User) {
        return CreateClient(CreateURL(SourceURL, HttpPort, false), User);
    }

    public AmazonS3 CreateTargetClient(UserData User) {
        return CreateClient(CreateURL(TargetURL, HttpPort, false), User);
    }

    public AmazonS3 CreateSourceClientHttps(UserData User) {
        return CreateClient(CreateURL(SourceURL, HttpsPort, true), User);
    }

    public AmazonS3 CreateTargetClientHttps(UserData User) {
        return CreateClient(CreateURL(TargetURL, HttpsPort, true), User);
    }

    public AccessControlList ACLGranteeClear(AccessControlList ACL) {
        List<Grant> GrantsList = ACL.getGrantsAsList();
        AccessControlList NewACL = new AccessControlList();
        for (Grant grant : GrantsList) {
            Grantee grantee = new CanonicalGrantee(grant.getGrantee().getIdentifier());
            NewACL.grantPermission(grantee, grant.getPermission());
        }
        NewACL.setOwner(ACL.getOwner());
        return NewACL;
    }

    public Date GetTimeToAddSeconds(int Seconds) {
        Calendar today = Calendar.getInstance();
        today.add(Calendar.SECOND, Seconds);

        return new Date(today.getTimeInMillis());
    }

    public String HttpToHttps(String Address) {
        return Address.replace("(?i)http", "https");
    }

    public boolean FolderCheck(String ObjectName) {
        if (ObjectName.endsWith("/"))
            return true;
        if (ObjectName.endsWith("\\"))
            return true;
        return false;
    }

    public InputStream CreateBody(String Body) {
        return new ByteArrayInputStream(Body.getBytes());
    }
}
