create or replace procedure "ambb"."p_rptnoofpicksloc" (
 iv_speciality in numeric,
 iv_doctorid in varchar default null,
 iv_driverid in varchar default null,
 iv_emt in varchar default null,
 iv_fromdate in date default null,
 iv_todate in date  default null 
)is 
 begin
    select * from dummy where id =(select 10*10);  

end ;