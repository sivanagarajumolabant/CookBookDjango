CREATE OR REPLACE PROCEDURE "HRPAY"."PR_CONNECT_BY_DATASET" (P_action int,ref out SYS_REFCURSOR)
IS
BEGIN
/* Created by Nagaraju */

if P_action=1 then
open ref for select public.CONNECT_BY_PRIOR('empno,mgr','emp',null,'empno=mgr');
ELSIF P_action=2 then
open ref for select public.CONNECT_BY_PRIOR('empno,mgr','emp','ename=''KING''','empno=mgr');
else
open ref for select 'please pass valid inputs' as status from dual;
end if;
end;














