options fullstimer;
/*  */

cas ses_main ;
caslib _all_ assign;

data casuser.paclaims 
	 casuser.cobmemsall 
	 casuser.pamems
	 casuser.oldmems
;
	array seed{3} 	_temporary_ (12345 13579 24680);
	array mu{3}	 	_temporary_ (19.1 23.5 41.7);
	array s2{3} 	_temporary_ (4.4 7.8 1.5) ;;
	array cl{10} $6 _temporary_ ('227851' '227852' '227853' '227854' '227855' '227856' '227857' '227858' '227859' '227850');
	do idnumber=1 to 1e5;
		a=1+mod(idnumber,57);
		StateName=fipname(a);
		StateAbbr=fipstate(a);
		backstate=reverse(statename);
		backstateabbr=reverse(stateabbr);
		sbmd_cg_at			= MU[1] + sqrt(S2[1]) * rannor(seed[1]);
		EACL_PRV_ALCRG_AT	= MU[2] + sqrt(S2[2]) * rannor(seed[2]);
		anothermetric		= MU[3] + sqrt(S2[3]) * rannor(seed[3]);
		cl_N				= cl[mod(idnumber,10)+1];
		EACM_WHS_UNQ_MBR_ID	= idnumber;
		output casuser.paclaims ;
		selmod = mod(idnumber,100);
		if selmod = 0 then do;
			surrogate_id = EACM_WHS_UNQ_MBR_ID;
		    output casuser.cobmemsall ;
		end;
		if selmod = 1 then do;
			surrogate_id = EACM_WHS_UNQ_MBR_ID;
		    output casuser.pamems;
		end;
		if selmod = 2 then do;
			surrogate_id = EACM_WHS_UNQ_MBR_ID;
		    output casuser.oldmems;
		end;
	end;
run;

/********************** Query 1 **********************/

%let timer_a = %sysfunc(datetime());

proc sql;
create table pp1 as 
select distinct 
sum(SBMD_CG_AT) as billed, 
sum(EACL_PRV_ALCRG_AT) as allowed 
from casuser.paclaims;
quit;
%let timer_b = %sysfunc(datetime());

proc fedsql sessref=ses_main;
	create table casuser.pp1  {options replace=true} as 
		select distinct 
		sum(SBMD_CG_AT) as billed, 
		sum(EACL_PRV_ALCRG_AT) as allowed 
		from casuser.paclaims;
quit;
%let timer_c = %sysfunc(datetime());

proc means data=casuser.paclaims nway noprint;
	output out=casuser.pp1_m(keep=billed allowed)  sum(sbmd_cg_at EACL_PRV_ALCRG_AT) = billed allowed;
run;

%let timer_d = %sysfunc(datetime());


proc cas;
   simple.summary /
      inputs={"sbmd_cg_at" "EACL_PRV_ALCRG_AT"},
      subset={"sum"},
      table={caslib="casuser",name="paclaims"},
      casout={caslib="casuser", name="pp1_cas", replace=true};
run;
quit;
%let timer_e = %sysfunc(datetime());

title CAS Actions;
proc print data=casuser.pp1_cas;
title;
data _null_;
	file print;
  dur1 = &timer_b - &timer_a;
  dur2 = &timer_c - &timer_b;
  dur3 = &timer_d - &timer_c;
  dur4 = &timer_e - &timer_d;
  put 30*'-' / '    PROC SQL DURATION:' dur1 time13.2 / ' PROC FEDSQL DURATION:' dur2 time13.2 / '  PROC MEANS DURATION:' dur3 time13.2/'  CAS Action DURATION:' dur4 time13.2/30*'-';
run;

/********************** Query 2 **********************/


%let timer_a = %sysfunc(datetime());
proc sql;
	create table work.pp2 as select distinct sum(SBMD_CG_AT) as billed, sum(EACL_PRV_ALCRG_AT) as allowed from casuser.paclaims
	where EACM_WHS_UNQ_MBR_ID in (select EACM_WHS_UNQ_MBR_ID from casuser.cobmemsall);
quit;

%let timer_b = %sysfunc(datetime());
proc fedsql sessref=ses_main;
	create table casuser.pp2  {options replace=true} as 
		select distinct 
			 sum(a.SBMD_CG_AT) as billed
			,sum(a.EACL_PRV_ALCRG_AT) as allowed 
		from casuser.paclaims a
		inner join casuser.cobmemsall b
		on a.EACM_WHS_UNQ_MBR_ID = b.surrogate_id
		;
quit;

%let timer_c = %sysfunc(datetime());
title PROC SQL;
proc print data=work.pp2;
title PROC FEDSQL;
proc print data=casuser.pp2;
run;
title;
data _null_;
	file print;
  dur1 = &timer_b - &timer_a;
  dur2 = &timer_c - &timer_b;
  put 30*'-' / '    PROC SQL DURATION:' dur1 time13.2 / ' PROC FEDSQL DURATION:' dur2 time13.2 /30*'-';
run;

/********************** Query 3 **********************/
%let timer_a = %sysfunc(datetime());
proc sql;
	create table pp3 as select distinct sum(SBMD_CG_AT) as billed, sum(EACL_PRV_ALCRG_AT) as allowed from casuser.paclaims
	where CL_N = '227852' and EACM_WHS_UNQ_MBR_ID not in (select EACM_WHS_UNQ_MBR_ID from casuser.cobmemsall);
quit;
%let timer_b = %sysfunc(datetime());
proc fedsql sessref=ses_main;
	create table casuser.pp3  {options replace=true} as 
		select distinct 
			 sum(a.SBMD_CG_AT) as billed
			,sum(a.EACL_PRV_ALCRG_AT) as allowed 
		from casuser.paclaims a
		full outer join casuser.cobmemsall b
		on a.EACM_WHS_UNQ_MBR_ID = b.surrogate_id
		where a.CL_N = '227852'
		;
quit;

%let timer_c = %sysfunc(datetime());
title PROC SQL;
proc print data=work.pp3;
title PROC FEDSQL;
proc print data=casuser.pp3;
run;
title;
data _null_;
	file print;
  dur1 = &timer_b - &timer_a;
  dur2 = &timer_c - &timer_b;
  put 30*'-' / '    PROC SQL DURATION:' dur1 time13.2 / ' PROC FEDSQL DURATION:' dur2 time13.2 /30*'-';
run;


/********************** Query 4 **********************/
%let timer_a = %sysfunc(datetime());
proc sql;
create table pp4 as 
	select distinct 
		 sum(SBMD_CG_AT) as billed
		,sum(EACL_PRV_ALCRG_AT) as allowed 
	from casuser.paclaims c 
	left join casuser.pamems m on c.EACM_WHS_UNQ_MBR_ID = m.EACM_WHS_UNQ_MBR_ID 
		and c.ICRD_DT between m.EFF_COV_DT and m.TERM_COV_DT and c.GP_ID = m.GP_ID
	where c.EACM_WHS_UNQ_MBR_ID in (select EACM_WHS_UNQ_MBR_ID from casuser.oldmems) 
	and c.CL_N <> '227852' 
	and c.EACM_WHS_UNQ_MBR_ID not in (select EACM_WHS_UNQ_MBR_ID from casuser.cobmemsall);
quit;



/* create table pp5 as select distinct sum(SBMD_CG_AT) as billed, sum(EACL_PRV_ALCRG_AT) as allowed from work.paclaims c */
/* left join work.pamems m on c.EACM_WHS_UNQ_MBR_ID = m.EACM_WHS_UNQ_MBR_ID and c.ICRD_DT between m.EFF_COV_DT and m.TERM_COV_DT */
/* and c.GP_ID = m.GP_ID */
/* where m.EACM_WHS_UNQ_MBR_ID is null */
/* and c.EACM_WHS_UNQ_MBR_ID not  in (select EACM_WHS_UNQ_MBR_ID from work.oldmems) and c.CL_N <> '227852' and c.EACM_WHS_UNQ_MBR_ID not in (select EACM_WHS_UNQ_MBR_ID from work.cobmemsall) */
/* ;quit; */
