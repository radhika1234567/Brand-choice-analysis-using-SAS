/*Generating PDF*/
ods html close;
options nodate nonumber;
ods pdf file='H:Radhika_Outputs.pdf' pdftoc=2;
LIBNAME qq 'H:\laundet';
data qq.a1;
infile 'H:\laundet\laundet_groc_1114_1165' firstobs =2;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR;RUN;
proc import datafile = 'H:\prod_laundet.csv'
DBMS=CSV
OUT=qq.A2;
GETNAMES=YES;RUN;
proc print data = pq.a2(obs=10);run;
proc contents data = a3;run;
data qq.a3;
infile 'H:\laundet\laundet_PANEL_GR_1114_1165.dat' truncover firstobs = 2 dlm='09'x;
input PANID WEEK UNITS OUTLET $ DOLLARS IRI_KEY  COLUPC  15.;
run;
proc print data=qq.a3(obs=10);run;
proc import datafile = 'H:\ads demo3.csv'
DBMS=CSV
OUT=qq.A4;
GETNAMES=YES;RUN;
data qq.a4(drop = Panelist_Type COUNTY MALE_SMOKE FEM_SMOKE Language HISP_FLAG HISP_CAT 
				Microwave_Owned_by_HH ZIPCODE FIPSCODE market_based_upon_zipcode 
				IRI_Geography_Number EXT_FACT Year HH_Head_Race__RACE2_); 
set qq.a4;
run;
proc print data=qq.a4(obs=10);run;
/*DATA qq.A5; 
INFILE 'H:\laundet\Delivery_Stores' FIRSTOBS = 2;
INPUT IRI_KEY OU $ EST_ACV Market_Name $ Open Clsd MskdName $;run;*/
/* Converting the upc into collapsed upc */
data qq.A1;
set qq.A1;
colupc = sy*100000000000+ge*10000000000+vend*100000+item;
run;
/* Converting the upc into collapsed upc */
data qq.a2;
set qq.a2;
colupc = sy*100000000000+ge*10000000000+vend*100000+item;
run;

PROC SQL;

CREATE TABLE qq.A7 AS
SELECT *
FROM qq.a1 A inner JOIN
qq.a2 B
ON A.colupc = B.colupc
ORDER BY A.DOLLARS desc;
QUIT;
proc print data = qq.a7 (obs=10);run;
PROC SQL;

CREATE TABLE qq.Top6 AS
SELECT L3,L4,L5, SUM(DOLLARs) as Sales
FROM qq.a7
GROUP BY L3,L4,L5
ORDER BY Sales desc;
QUIT;

Proc print data = qq.Top6 (obs = 3); run;
/*Subsetting Wisk and top 3*/
Data qq.SUBSET;set qq.A7;If L5 ='WISK' or L5 = 'TIDE' or L5 ='ALL' or L5='PUREX';run;
proc print data = qq.subset(obs = 10);run;

/* Handling the dataset: 
1. Choosing Brands
2. Calculating total volume
3. Defining PPO (Price Per fluid Once)
4. Converting feature variable into binary value
*/
data qq.subset;
format brand $char13.;
set qq.subset;
if index(L5,'TIDE') then brand = 'TIDE';
else if index(L5,'ALL') then brand = 'ALL';
else if index(L5,'PUREX') then brand = 'PUREX';
else if index(L5,'WISK') then brand = 'WISK';
TOT_VOL = UNITS * VOL_EQ * 16;
PPO = round(DOLLARS/TOT_VOL,.01); *Price Per fluid Once;
if F eq 'NONE' then Feature=0; else Feature=1;
run;
data qq.subset(drop = VOL_EQ F);
set qq.subset;
run;
/* What percent of the dataset contains which brand?*/
proc freq data=qq.subset;
table brand;
run;
/*Calculating revenue and market share for each brand*/
proc sql;
create table qq.revenuet as 
select brand, Sum(dollars) as Revenue
from qq.subset
group by brand
order by Revenue desc;
quit;
proc sql;
create table qq.market_s as
select brand, Revenue, round(Revenue/Sum(Revenue),0.01) as mkt_share
from qq.revenuet
order by mkt_share desc;
quit;
proc print data = qq.market_s(obs=10);run;
proc sort data=qq.subset out=qq.subset;
by brand;
run;
proc sort data=qq.market_s out=qq.market_s;
by brand;
run;
/* Merging subset and market_s to include market share value into the dataset*/
data qq.subset;
merge qq.subset(in=x) qq.market_s(in=y);
by brand;
if x=1 and y=1;
run;
proc print data = qq.subset(obs=10);run;
/* Market Share value for each brand */
proc tabulate data=qq.subset;
class brand;
var mkt_share;
table brand, mkt_share * mean;
run;
proc tabulate data=qq.subset;
class brand;
var feature;
table brand, feature * mean;
run;
proc tabulate data=qq.subset;
class brand;
var d;
table brand, d * mean;
run;
proc tabulate data=qq.subset;
class brand;
var PPO;
table brand, PPO * mean;
run;
/* Applying weight to Display and Feature */
data qq.subset;
set qq.subset;
disp = D * mkt_share;
features = Feature * mkt_share;
drop D Feature;
run;
/* Calculating 'average weekly PPO' for each brand and store*/
proc sql;
create table qq.X1 as 
select brand, iri_key, week, round(avg(ppo),0.01) as weekly_PPO
from qq.subset
group by iri_key, week, brand
order by iri_key, week;
quit;
/* Converting dataset into wide format:
To create new variables for PPO, Display, and feature for each brand
*/
proc transpose data = qq.X1 out = qq.X1_wide (drop=_NAME_) prefix=PPO_;
	by iri_key week;
	var weekly_PPO;
	id Brand;
run;
proc sql;
create table qq.X2 as 
select brand, iri_key, week, round(sum(disp),0.01) as weekly_D
from qq.subset
group by iri_key, week, brand
order by iri_key, week;
quit;
proc transpose data = qq.X2 out = qq.X2_wide (drop=_NAME_) prefix=D_;
	by iri_key week;
	var weekly_D;
	id Brand;
run;
proc sql;
create table qq.X3 as 
select brand, iri_key, week, round(sum(features),0.01) as weekly_F
from qq.subset
group by iri_key, week, brand
order by iri_key, week;
quit;
proc transpose data = qq.X3 out = qq.X3_wide (drop=_NAME_) prefix=F_;
	by iri_key week;
	var weekly_F;
	id Brand;
run;
data qq.x1_wide;
 set qq.x1_wide;
 if cmiss(of _all_) then delete;
run;
data qq.X2_wide;
 set qq.X2_wide;
 if cmiss(of _all_) then delete;
run;
data qq.X3_wide;
 set qq.X3_wide;
 if cmiss(of _all_) then delete;
run;
data qq.X4;
merge qq.X1_wide(in=x) qq.X2_wide(in=y);
by iri_key week;
if x=1 and y=1;
run;
data qq.X5;
merge qq.X4(in=x) qq.X3_wide(in=y);
by iri_key week;
if x=1 and y=1;
run;
proc print data=qq.X5(obs=12);run;
proc print data=qq.subset(obs=12);run;
/*joining subset with panel data*/
proc sort data=qq.subset out=qq.subset;
by colupc;
run;
proc sort data=qq.a3 out=qq.a3;
by colupc;
run;
proc contents data= qq.subset;run;
proc contents data=qq.a3;run;
data qq.panel_sub;
merge qq.a3(in=x) qq.subset(in=y keep= colupc brand);
by colupc;
if x=1 and y=1;
run;
proc print data=qq.panel_sub(obs=10);run;
proc sql;
create table qq.panel as 
select PANID, IRI_KEY, WEEK, Brand , sum(Units) as Num_PurchasedItems, sum(Dollars) as Payment 
from qq.panel_sub
group by PANID, IRI_KEY, WEEK, Brand ;
quit;
proc sort data=qq.panel out=qq.panel;
by iri_key week;
run;
proc sort data=qq.X5 out=qq.X5;
by iri_key week;
run;
/* Merging subset and panel data */
data qq.panel;
merge qq.panel(in=x) qq.X5(in=y);
by iri_key week;
if x=1 and y=1;
run;
proc print data = qq.panel(obs=10);
where brand = 'TIDE';
run;
proc sort data=qq.panel out=qq.panel;
by panid;
run;
proc sort data=qq.a4 out=qq.a4;
by panelist_id;
run;
data qq.panel;
merge qq.panel(in=x) qq.a4(in=y rename=(panelist_id=panid));
by panid;
if x=1 and y=1;
run;

data qq.panel;
set qq.panel;
if brand = 'TIDE' then br = 1;
if brand = 'ALL' then br = 2;
if brand = 'PUREX' then br = 3;
if brand = 'WISK' then br = 4;
run;
proc contents data=qq.panel; run;
data qq.panel1 (keep= pid decision mode price display features family_size Combined_Pre_Tax_Income_of_HH HH_Race HH_Head_Race__RACE3_ Type_of_Residential_Possession);
set qq.panel;
array pvec{4} PPO_TIDE PPO_ALL PPO_PUREX PPO_WISK;
array dvec{4} D_TIDE D_ALL D_PUREX D_WISK;
array fvec{4} F_TIDE F_ALL F_PUREX F_WISK;
retain pid 0;
pid+1;
do i = 1 to 4;
	mode=i;
	price=pvec{i};
	display=dvec{i};
	features=fvec{i};
	decision=(br=i);
	output;
end;
run;
proc print data = qq.panel1 (obs=20);
where decision=1;
run;
/* Creating dummy variables and interaction terms*/
data qq.panel1;
set qq.panel1;
brand2=0;
brand3=0;
brand4=0;
if mode = 2 then brand2 = 1;
if mode = 3 then brand3 = 1;
if mode = 4 then brand4 = 1;
family_size2 = family_size * brand2;
family_size3 = family_size * brand3;
family_size4 = family_size * brand4;
Combined_Pre_Tax_Income_of_HH2 = Combined_Pre_Tax_Income_of_HH * brand2;
Combined_Pre_Tax_Income_of_HH3 = Combined_Pre_Tax_Income_of_HH * brand3;
Combined_Pre_Tax_Income_of_HH4 = Combined_Pre_Tax_Income_of_HH * brand4;
HH_Race2 = HH_Race*brand2;
HH_Race3 = HH_Race*brand3;
HH_Race4 = HH_Race*brand4;
Type_of_Residential_Possession2 = Type_of_Residential_Possession* brand2;
Type_of_Residential_Possession3 = Type_of_Residential_Possession* brand3;
Type_of_Residential_Possession4 = Type_of_Residential_Possession* brand4;
int_pd2 = price*display*brand2;
int_pd3 = price*display*brand3;
int_pd4 = price*display*brand4;
int_pf2 = price*features*brand2;
int_pf3 = price*features*brand3;
int_pf4 = price*features*brand4;
run;
proc means data=qq.panel1 nmiss n;run;
proc print data = qq.panel1(obs=10);run;
 /*MDC model - without interaction terms */

proc mdc data=qq.panel1;
model decision = price display features family_size2-family_size4 Combined_Pre_Tax_Income_of_HH2-Combined_Pre_Tax_Income_of_HH4 Type_of_Residential_Possession2-Type_of_Residential_Possession4 HH_Race2-HH_Race4/ type=clogit 
	nchoice=4
    optmethod=qn
    covest=hess;
	id pid;
	output out=probdata pred=p;
run;
/* MDC model - without interaction term price*display */
proc mdc data=qq.panel1;
model decision = brand2 brand3 brand4 price display features family_size2-family_size4 Combined_Pre_Tax_Income_of_HH2-Combined_Pre_Tax_Income_of_HH4 Type_of_Residential_Possession2-Type_of_Residential_Possession4 HH_Race2-HH_Race4 int_pd2-int_pd4/ type=clogit 
	nchoice=4
    optmethod=qn
    covest=hess;
	id pid;
run;
/* MDC model - without interaction term price*feature */
proc mdc data=qq.panel1;
model decision = brand2 brand3 brand4 price display features family_size2-family_size4 Combined_Pre_Tax_Income_of_HH2-Combined_Pre_Tax_Income_of_HH4 Type_of_Residential_Possession2-Type_of_Residential_Possession4 HH_Race2-HH_Race4 int_pf2-int_pf4/ type=clogit 
	nchoice=4
    optmethod=qn
    covest=hess;
	id pid;
run;

ods pdf close;


