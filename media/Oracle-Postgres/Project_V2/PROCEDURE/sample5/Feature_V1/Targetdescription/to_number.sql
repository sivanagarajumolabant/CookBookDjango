CREATE OR REPLACE PROCEDURE pr_tonumber RETURN DATE IS
   DECLARE
      DATE deadline;
   BEGIN
   select to_char(123)from dual;
   select to_char(123)from dummy;
   select to_number('123')from dual;

   SELECT
  TO_DATE( '5 Jan 2017', 'DD MON YYYY' )
FROM
  dual;
      SELECT MAX(start_date + duration) INTO deadline FROM projects;
      RETURN deadline;
   END;