CREATE OR REPLACE FUNCTION getnodedetailsforqcapproval(in nodetype int4, in activity int4, in types int4, in enterid varchar)
  RETURNS TABLE (responselogid int4, consumerno varchar, consumername text, nodetypes varchar, activitys varchar, qcstatus text, responsedate varchar, responsetime varchar, nodeproperty1 text, nodeproperty2 text) AS 
$BODY$
declare distnodeid int4; rlid int4;

begin

insert into assignmentpreelogs(fntext,created) select 'getnodedetailsforqcapproval'||$1::text||'/'||$2::text||'/'||$3::text||'/'||$4::text,now();

if $3=1 then 
    begin
--               if exists (select 1 from tblresponselogs r where r.responselogid=$4::int4 and r.nodetype=$1 and r.activityid=$2 ) then
        
                return query(
                select a.responselogid, a.consumer_number, a.consumer_name, a.nodetypes, a.activity,  (case when a.qcstatus ilike '%Intellismart%' then 'Pending@QC Intellismart' else a.qcstatus end) qcstatus
                , a.responsedate, a.responsetime, a.nodeproperty1, a.nodeproperty2
                from
                (      select r.responselogid,distcode consumer_number,_19 consumer_name,dt.distributionnodetypename nodetypes,at.description activity
                        ,(case when r.responsestatusid in(1) then 'Approved' when r.responsestatusid=27 then 'Consumer Denied Access'  
                               when r.responsestatusid=3 then 'Pending@Resurvey'  when r.responsestatusid=7 then 'Pending@'||discription
                               when r.nextapproverroleid=11 then 'Pending@QC Contractor' when r.nextapproverroleid in (9,78) then 'Pending@Intellismart' when r.nextapproverroleid=10 then 'Pending@QC Discom'
                               when r.nextapproverroleid=68 then 'Pending@PESL'
                           else ''  end) qcstatus,r.responsedate,r.responsetime
                        , (case when r.nodetype=1 then 'Consumer Number' when r.nodetype=7 then 'Intellismart DT Code' else '' end) nodeproperty1
                       , (case when r.nodetype=1 then 'Consumer Name' when r.nodetype=7 then 'DTR Name' else '' end) nodeproperty2
                        from tblresponselogs r
                        left join etl_masterdata m on r.distnid=m.distributionnodeid
                        left join tbldistributionnodetypes dt on r.nodetype=dt.distributionnodetypeid
                        left join tblactivitynew at on r.activityid=at.activityid
                        left join tblprojectuserallocation up on r.nextapproverid=up.userid and up.projectid=9
                        left join tblrole rl on up.roleid=rl.roleid
                        where r.responselogid=$4::int4 and r.activityid=$2 and r.nodetype=$1 and r.projectid<>999) as a
                );

    end;

elsif $3=2 then 
    begin

                select distributionnodeid into distnodeid 
                from tbldistributionnodes
                where distributionnodecode=$4 and distributionnodetypeid=$1;

                select max(rm.responselogid) into rlid 
                from tblresponselogs rm
                where rm.distnid=distnodeid and rm.activityid=$2 and rm.projectid<>999;

--               if exists (select 1 from tblresponselogs r where r.distcode=$4 and r.nodetype=$1 and r.activityid=$2 ) then

                return query(
                select a.responselogid, a.consumer_number, a.consumer_name, a.nodetypes, a.activity,  (case when a.qcstatus ilike '%Intellismart%' then 'Pending@QC Intellismart' else a.qcstatus end) qcstatus
                , a.responsedate, a.responsetime, a.nodeproperty1, a.nodeproperty2
                from
                (      select r.responselogid,distcode consumer_number,_19 consumer_name,dt.distributionnodetypename nodetypes,at.description activity
                        ,(case when r.responsestatusid in(1) then 'Approved' when r.responsestatusid=27 then 'Consumer Denied Access'  
                               when r.responsestatusid=3 then 'Pending@Resurvey'  when r.responsestatusid=7 then 'Pending@'||discription
                               when r.nextapproverroleid=11 then 'Pending@QC Contractor' when r.nextapproverroleid in (9,78) then 'Pending@Intellismart' when r.nextapproverroleid=10 then 'Pending@QC Discom'
                               when r.nextapproverroleid=68 then 'Pending@PESL'
                           else ''  end) qcstatus,r.responsedate,r.responsetime
                        , (case when r.nodetype=1 then 'Consumer Number' when r.nodetype=7 then 'Intellismart DT Code' else '' end) nodeproperty1
                       , (case when r.nodetype=1 then 'Consumer Name' when r.nodetype=7 then 'DTR Name' else '' end) nodeproperty2
                        from tblresponselogs r
                        left join etl_masterdata m on r.distnid=m.distributionnodeid
                        left join tbldistributionnodetypes dt on r.nodetype=dt.distributionnodetypeid
                        left join tblactivitynew at on r.activityid=at.activityid
                        left join tblprojectuserallocation up on r.nextapproverid=up.userid and up.projectid=9
                        left join tblrole rl on up.roleid=rl.roleid
                        where r.responselogid= rlid) as a

                );

    end;

end if;
 
end;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER