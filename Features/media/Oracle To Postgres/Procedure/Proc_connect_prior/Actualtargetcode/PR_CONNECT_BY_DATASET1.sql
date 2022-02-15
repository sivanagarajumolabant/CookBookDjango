CREATE OR REPLACE PROCEDURE "HRPAY"."PR_CONNECT_BY_DATASET1" (ref out SYS_REFCURSOR)
IS
BEGIN
open ref for select public.CONNECT_BY_PRIOR('empno,ename,job,deptno,mgr','emp','ename=''KING''','empno=mgr');
end;














