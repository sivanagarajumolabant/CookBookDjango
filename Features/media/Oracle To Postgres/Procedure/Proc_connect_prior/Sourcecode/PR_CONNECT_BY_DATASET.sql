CREATE OR REPLACE PROCEDURE "HRPAY"."PR_CONNECT_BY_DATASET" (P_action int,ref out SYS_REFCURSOR)
IS
BEGIN
/* Created by Nagaraju */

if P_action=1 then
open ref for select empno,mgr,level from emp connect by prior empno=mgr order by level;
ELSIF P_action=2 then
open ref for select empno,mgr,level from emp connect by prior empno=mgr start with ename='KING' order by level;
else
open ref for select 'please pass valid inputs' as status from dual;
end if;
end;










