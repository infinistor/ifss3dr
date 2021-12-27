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
package com.pspace.ifs.Portal;

import java.util.ArrayList;
import java.util.List;

public class PortalUserData {
    public String Id;
    public String LoginId;
    public String Email;
    public String Name;
    public String DisplayName;
    public String PasswordChangeDate;
    public String PhoneNumber;
    public Boolean ReceiveSms;
    public Boolean ReceiveEmail;
    public List<String> Roles;
    public String LastLoginDateTIme;
    public String ProductType;

    public PortalUserData()
    {
        Roles = new ArrayList<String>();
        Init();
    }

    public void Init()
    {
        Id = "";
        LoginId = "";
        Email = "";
        Name = "";
        DisplayName = "";
        PasswordChangeDate = "";
        PhoneNumber = "";
        ReceiveSms = false;
        ReceiveEmail = false;
        Roles.clear();
        LastLoginDateTIme = "";
        ProductType = "";
    }

    @Override
    public String toString()
    {
        return String.format(
        "%s{\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s,\n" + 
            "\t%s : %s\n" + 
        "}",
        "PortalUserData",
            "Id", Id,
            "LoginId", LoginId,
            "Email", Email,
            "Name", Name,
            "DisplayName", DisplayName,
            "PasswordChangeDate", PasswordChangeDate,
            "PhoneNumber", PhoneNumber,
            "ReceiveSms", ReceiveSms.toString(),
            "ReceiveEmail", ReceiveEmail.toString(),
            "Roles", Roles.toString(),
            "LastLoginDateTIme", LastLoginDateTIme,
            "ProductType", ProductType
        );
    }
}
