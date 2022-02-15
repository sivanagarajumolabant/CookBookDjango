CREATE OR REPLACE PROCEDURE "HRPAY"."PR_CONNECT_BY_DATASET1" (ref out SYS_REFCURSOR)
IS
BEGIN
open ref for select empno,ename,job,deptno,mgr,level from emp connect by prior empno=mgr start with ename='KING' order by level;
end;










