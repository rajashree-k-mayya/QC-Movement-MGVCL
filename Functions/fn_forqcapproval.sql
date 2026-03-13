CREATE OR REPLACE FUNCTION fn_forqcapproval(in nodetype int4, in activity int4, in types int4, in enterid varchar, in fromqcrid int4, in toqcrid int4, in approvelog jsonb, in approveremark text)
  RETURNS TABLE (status int4, msg text) AS 
$BODY$
declare distnodeid int4; rlid int4; inputrlid varchar;  qcinteliuserid int4; qcinteliapprovedate timestamp; qcpgcluserid int4; qcpgclapprovedate timestamp; 
qcdiscomuserid int4; qcdiscomapprovedate timestamp; qcconuserid int4; qcconapprovedate timestamp; maxsdid int4; resid int4; distrid int4; times text; lastid int4;

begin

insert into assignmentpreelogs(fntext,created) select 'fn_forqcapproval'||$1::text||'/'||$2::text||'/'||$3::text||'/'||$4::text||'/'||$5::text||'/'||$6::text||'/'||$7::text||'/'||$8::text,now();


SELECT to_char(NOW(), 'HH24:MI:SS')::text into times;


CREATE TEMP TABLE IF NOT EXISTS tmp_qc_data (
    qcrid INT,
    username TEXT,
    approved_date DATE
) ON COMMIT DROP;

--IF NOT EXISTS (SELECT 1 FROM tmp_qc_data) THEN

INSERT INTO tmp_qc_data (qcrid, username, approved_date)
SELECT
    (x->>'qcRId')::INT,
    x->>'approved_username',
    TO_DATE(x->>'approved_date', 'DD-MM-YYYY')
FROM jsonb_array_elements(approvelog) x;


SELECT  tu.userid into qcconuserid
FROM tmp_qc_data td
inner join tblusers tu on td.username=tu.username
WHERE qcrid = 1;

SELECT  (td.approved_date||' '||times)::timestamp into qcconapprovedate
FROM tmp_qc_data td
WHERE qcrid = 1;

SELECT  tu.userid into qcinteliuserid
FROM tmp_qc_data td
inner join tblusers tu on td.username=tu.username
WHERE qcrid = 2;

SELECT (td.approved_date||' '||times)::timestamp into qcinteliapprovedate
FROM tmp_qc_data td
WHERE qcrid = 2;


SELECT  tu.userid into qcpgcluserid
FROM tmp_qc_data td
inner join tblusers tu on td.username=tu.username
WHERE qcrid = 3;

SELECT (td.approved_date||' '||times)::timestamp into qcpgclapprovedate
FROM tmp_qc_data td
WHERE qcrid = 3;

SELECT  tu.userid into qcdiscomuserid
FROM tmp_qc_data td
inner join tblusers tu on td.username=tu.username
WHERE qcrid = 4;

SELECT  (td.approved_date||' '||times)::timestamp into qcdiscomapprovedate
FROM tmp_qc_data td
WHERE qcrid = 4;


if $3=1 then

               rlid := $4::int4;

elsif $3=2 then

                select distributionnodeid into distnodeid 
                from tbldistributionnodes
                where distributionnodecode=$4 and distributionnodetypeid=$1;

                select max(rm.responselogid) into rlid 
                from tblresponselogs rm
                where rm.distnid=distnodeid and rm.activityid=$2 and rm.projectid<>999; 

end if;

if $3 in (1,2) then

                inputrlid := rlid::varchar;

begin
    --Pending@QC Contractor->Pending@QC Intellismart CI
    if $5=1 and $6=2 and $2 in (71,43) then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 5,nextapproverroleid = 78, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcconapprovedate,now()), coalesce (qcconuserid,12), 5, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcconuserid,12), coalesce (qcconapprovedate,now()) ,  $8 );


    end;
    end if;

    --Pending@QC Contractor->Pending@QC Intellismart MI
    if $5=1 and $6=2 and $2 in (72,44)  then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 5,nextapproverroleid = 9, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcconapprovedate,now()), coalesce (qcconuserid,12), 5, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcconuserid,12), coalesce (qcconapprovedate,now()) ,  $8 );


    end;
    end if;

    --Pending@QC Intellismart MI->Pending@QC RECPDCL/PESL
    if $5=2 and $6=3 then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 6,nextapproverroleid = 68, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;


            if $2 in (71,43) then 
                    INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
                    select rlid,coalesce (qcinteliapprovedate,now()), coalesce (qcinteliuserid,329), 6, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcinteliuserid,329), coalesce (qcinteliapprovedate,now()) ,  $8 );

            elsif $2 in (72,44) then
                    INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
                    select rlid,coalesce (qcinteliapprovedate,now()), coalesce (qcinteliuserid,13), 6, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcinteliuserid,13), coalesce (qcinteliapprovedate,now()) ,  $8 );

            end if;

    end;
    end if;

    --Pending@QC PESL->Pending@QC Discom
    if $5=3 and $6=4 then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 28,nextapproverroleid = 10, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcpgclapprovedate, now()), coalesce (qcpgcluserid,409), 28, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcpgcluserid,409), coalesce (qcpgclapprovedate,now()) ,  $8 );

    end;
    end if;

    --discom->approve
    if $5=4 and $6=6 then 
    begin

          	Select distnid into distrid from tblresponselogs where responselogid=rlid;

            select  (case when $2=71 then 72 when $2 = 72 then -1 when $2=43 then 44 when $2 = 44 then -1 else -1 end ) into lastid;

            UPDATE tblresponselogs r
            SET responsestatusid = 1,nextapproverroleid = 0, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcdiscomapprovedate,now()), coalesce (qcdiscomuserid,15), 1, $8;

            UPDATE tbldistributionnodes SET lastactivityid =lastid  WHERE distributionnodeid=distrid;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcdiscomuserid,15), coalesce (qcdiscomapprovedate,now()) ,  $8 );


    end;
    end if;

    --Pending@QC Contractor->Pending@PESL
    if $5=1 and $6=3 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 2,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);

    end;
    end if;

    --Pending@QC Contractor->Pending@QC Discom
    if $5=1 and $6=4 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 2,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 4,$7,$8);

    end;
    end if;

    --Pending@QC Contractor->Approved
    if $5=1 and $6=6 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 2,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 6,$7,$8);

    end;
    end if;

    --Pending@QC Intellismart->Pending@QC Contractor
    if $5=2 and $6=1 then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 0,nextapproverroleid = 11, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcinteliapprovedate,now()), coalesce (qcinteliuserid,0), 11, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcinteliuserid,0), coalesce (qcinteliapprovedate,now()) ,  $8 );

    end;
    end if;

    --Pending@PESL->Pending@QC Intellismart
    if $5=3 and $6=2 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 2,$7,$8);

    end;
    end if;

    --Pending@QC Discom->Pending@PESL
    if $5=4 and $6=3 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);

    end;
    end if;

    --Pending@QC Intellismart->Pending@QC Discom
    if $5=2 and $6=4 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 4,$7,$8);

    end;
    end if;

    --Pending@QC Intellismart->Approved
    if $5=2 and $6=6 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 6,$7,$8);

    end;
    end if;

    --Pending@PESL->Pending@QC Contractor
    if $5=3 and $6=1 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 2,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 1,$7,$8);

    end;
    end if;

    --Pending@PESL->Approved
    if $5=3 and $6=6 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 6,$7,$8);

    end;
    end if;

    --Pending@QC Discom->Pending@QC Contractor
    if $5=4 and $6=1 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 1,$7,$8);

    end;
    end if;

    --Pending@QC Discom->Pending@QC Intellismart
    if $5=4 and $6=2 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 3,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 2,$7,$8);

    end;
    end if;

    --Approved->Pending@QC Discom
    if $5=6 and $6=4 then 
    begin

            UPDATE tblresponselogs r
            SET responsestatusid = 28,nextapproverroleid = 10, updated = true
            WHERE responselogid = rlid and activityid=$2 and r.nodetype=$1;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcdiscomapprovedate,now()), coalesce (qcdiscomuserid,0), 28, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcdiscomuserid,0), coalesce (qcdiscomapprovedate,now()) ,  $8 );

    end;
    end if;

    --Approved->Pending@PESL
    if $5=6 and $6=3 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 6, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 3,$7,$8);


    end;
    end if;

    --Approved->Pending@QC Intellismart
    if $5=6 and $6=2 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 6, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 2,$7,$8);

    end;
    end if;

    --Approved->Pending@QC Contractor
    if $5=6 and $6=1 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 6, 4,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 1,$7,$8);

    end;
    end if;

    --Pending@QC Contractor->Pending@Resurvey
    if $5=1 and $6=5 then 
    begin

          	Select distnid into distrid from tblresponselogs where responselogid=rlid ;

         	select max(surveydetailsid) into maxsdid from tblsurveydetaails where responselogid=rlid;

        	update tblresponselogs set resurvey=1, responsestatusid = 3, rejectreason = $8 where responselogid=rlid;

        	INSERT INTO tblresurvey (distid, olid, resurvey, responseid) 
        	select distrid,(select distinct officelocationallocationid from tbldistributionnodelocationallocation 
        	where distributionnodeid=distrid ), 0, rlid ;
	
        	INSERT INTO tblresurveydetaails (distributionnodeid, olid, surveyid, surveydate, surveytime, walkalongseries, surveytype, latitude, longitude, surveyorname, responselogid, resurveyid) 
        	select distributionnodeid,olid,surveyid,surveydate,surveytime,walkalongseries,surveytype,latitude,longitude,surveyorname,responselogid ,(select max(resurveyid) from tblresurvey where responseid=rlid)
        	from tblsurveydetaails where responselogid=rlid and surveydetailsid=maxsdid returning resurveydetailsid into resid;
	
            INSERT INTO tblresurveyproperties (resurveydetailsid, catogorypropertyallocationid,orderid) 
            select resid,categorypropertyallocationid,0
            from tblresponselogs b
            cross join lateral jsonb_to_recordset(b.response::jsonb->'propertiesBean') as items (categorypropertyallocationid int4,value text) 
            where activityid=$2 and responsedate is not null and response<>'' and value<>'' and  responselogid=rlid ;

             INSERT INTO tblresurveypropertyvaluedetails (resurveypropertyid, value,valueid)
            with mctea as (
            select  distinct value,responselogid,categorypropertyallocationid,"valueId" valueid
                          from tblresponselogs b
                          cross join lateral jsonb_to_recordset(b.response::jsonb->'propertiesBean') as items (categorypropertyallocationid int4,value text,"valueId" int4) 
                          where activityid=$2 and responsedate is not null and response<>'' and value<>'' and responselogid=rlid and value<>'{"mobile_number":null,"validated":null}'
            )
            ,cteas as (
            select resurveypropertyid,catogorypropertyallocationid,s.responselogid from tblresurveydetaails s
            inner join tblresurveyproperties p on p.resurveydetailsid=s.resurveydetailsid
            )
            select resurveypropertyid,value,valueid from mctea a
            inner join cteas b on b.catogorypropertyallocationid=a.categorypropertyallocationid and a.responselogid=b.responselogid;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,coalesce (qcconapprovedate,now()), coalesce (qcconuserid,0), 3, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, coalesce (qcconuserid,0), coalesce (qcconapprovedate,now()) ,  $8 );

    end;
    end if;

    --Pending@QC Intellismart->Pending@Resurvey
    if $5=2 and $6=5 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 2, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 5,$7,$8);


    end;
    end if;

    --Pending@PESL->Pending@Resurvey
    if $5=3 and $6=5 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 3, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 5,$7,$8);


    end;
    end if;

    --Pending@QC Discom->Pending@Resurvey
    if $5=4 and $6=5 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 4, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 5,$7,$8);


    end;
    end if;

    --Pending@Resurvey->Pending@QC Contractor
    if $5=5 and $6=1 then 
    begin

          	Select distnid into distrid from tblresponselogs where responselogid=rlid ;

            update tblresurvey set resurvey=1 where distid = distrid;

            update tblresponselogs set updated=true,responsestatusid=0,nextapproverroleid=11,resurvey=0, approved = 0, rejectreason = ''
            where responselogid =rlid;

            INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks)
            select rlid,now(),0, 11, $8;

            INSERT INTO tblqcmovementlog ( nodetype, selectedby,  enteredvalue,  activity,  fromqclevel,  toqclevel,  movementdoneby,  movementdonedate, reason)
            VALUES ( $1, (case when $3=1 then 'sequenceno' when $3=2 then 'consumer number' else '' end),  $4, $2, $5, $6, 0, now() ,  $8 );

    end;
    end if;

    --Pending@Resurvey->Pending@QC Intellismart
    if $5=5 and $6=2 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 5, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 2,$7,$8);


    end;
    end if;

    --Pending@Resurvey->Pending@PESL
    if $5=5 and $6=3 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 5, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 3,$7,$8);


    end;
    end if;

    --Pending@Resurvey->Pending@QC Discom
    if $5=5 and $6=4 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 5, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 4,$7,$8);


    end;
    end if;

    --Pending@Resurvey->Approved
    if $5=5 and $6=6 then 
    begin

                Perform fn_forqcapproval($1, $2, 1, inputrlid, 5, 1,$7,$8);
                Perform fn_forqcapproval($1, $2, 1, inputrlid, 1, 6,$7,$8);

    end;
    end if;

       Perform sp_processresponse_1(9, $1, $2, rlid);


    return query (select 1,'success');

end;
end if;




end;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER