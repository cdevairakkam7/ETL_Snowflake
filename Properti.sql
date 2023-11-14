-- Create Database in Snowflake
CREATE DATABASE PROPERTI;

-- Create Schema in PROPERTI Database
create schema properti_schema;

-- Create raw codes table 
create table raw_codes_table(
    CECODID varchar,
    CMERKM varchar,	
    CODTXTLD varchar,	
    CODTXTKD varchar,	
    CODTXTLF varchar,	
    CODTXTKF varchar,	
    CODTXTLI varchar,	
    CODTXTKI varchar,	
    CEXPDAT varchar);

-- Create refined codes table
create table REFINED_CODES_TABLE (
	CECODID number,
	CMERKM VARCHAR(16777216),
	CODTXTLD VARCHAR(16777216),
	CODTXTKD VARCHAR(16777216),
	CODTXTLF VARCHAR(16777216),
	CODTXTKF VARCHAR(16777216),
	CODTXTLI VARCHAR(16777216),
	CODTXTKI VARCHAR(16777216),
	CEXPDAT Date
);

-- Create Integration between S3 & Snowflake for codes table
CREATE STORAGE INTEGRATION codes_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::374841118319:role/service-role/lambda_basic_execution'
  STORAGE_ALLOWED_LOCATIONS = ('s3://chris-properti-ftp/latest/');

-- Create an external Snowflake stage
CREATE STAGE s3_stage
  URL = 's3://chris-properti-ftp/latest/'
  STORAGE_INTEGRATION = codes_integration;

-- Create a Snowpipe
create or replace pipe code_pipe auto_ingest=true as
  copy into raw_codes_table
  from @s3_stage
  file_format = (format_name = 'swiz_csv_format')
  pattern ='.*kodes.*';

-- Populate values in refined codes table
insert into refined_codes_table 
select cecodid::number,
       CMERKM,
       CODTXTLD,
       CODTXTKD,
       CODTXTLF,
       CODTXTKF,
       CODTXTLI,
       CODTXTKI,
       split(CEXPDAT,'.')[2]||'-'||split(CEXPDAT,'.')[1]||'-'||split(CEXPDAT,'.')[0] as CEXPDAT
              
from raw_codes_table;

-- Create eingang_entree_entrata_table
create table raw_eingang_entree_entrata_table 
(EGID varchar,
EDID varchar,
EGAID varchar,
DEINR varchar,
ESID varchar,
STRNAME varchar,
STRNAMK varchar,
STRINDX varchar,
STRSP varchar,
STROFFIZIEL varchar,
DPLZ4 varchar,
DPLZZ varchar,
DPLZNAME varchar,
DKODE varchar,
DKODN varchar,
DOFFADR varchar,
DEXPDAT varchar);


-- Create refined_eingang_entree_entrata_table
create table refined_eingang_entree_entrata_table 
(EGID number,
EDID number,
EGAID number,
DEINR varchar,
ESID number,
STRNAME varchar,
STRNAMK varchar,
STRINDX varchar,
STRSP number,
STROFFIZIEL number,
DPLZ4 number,
DPLZZ number,
DPLZNAME varchar,
DKODE varchar,
DKODN varchar,
DOFFADR number,
DEXPDAT date);

-- Create raw_gebaeude_batiment_edificio_table
create table raw_gebaeude_batiment_edificio_table
(
    EGID       varchar,
    GDEKT      varchar,
    GGDENR     varchar,
    GGDENAME   varchar,
    EGRID      varchar,
    LGBKR      varchar,
    LPARZ      varchar,
    LPARZSX    varchar,
    LTYP       varchar,
    GEBNR      varchar,
    GBEZ       varchar,
    GKODE      varchar,
    GKODN      varchar,
    GKSCE      varchar,
    GSTAT      varchar,
    GKAT       varchar,
    GKLAS      varchar,
    GBAUJ      varchar,
    GBAUM      varchar,
    GBAUP      varchar,
    GABBJ      varchar,
    GAREA      varchar,
    GVOL       varchar,
    GVOLNORM   varchar,
    GVOLSCE    varchar,
    GASTW      varchar,
    GANZWHG    varchar,
    GAZZI      varchar,
    GSCHUTZR   varchar,
    GEBF       varchar,
    GWAERZH1   varchar,
    GENH1      varchar,
    GWAERSCEH1 varchar,
    GWAERDATH1 varchar,
    GWAERZH2   varchar,
    GENH2      varchar,
    GWAERSCEH2 varchar,
    GWAERDATH2 varchar,
    GWAERZW1   varchar,
    GENW1      varchar,
    GWAERSCEW1 varchar,
    GWAERDATW1 varchar,
    GWAERZW2   varchar,
    GENW2      varchar,
    GWAERSCEW2 varchar,
    GWAERDATW2 varchar,
    GEXPDAT    varchar
);


-- Create refined_gebaeude_batiment_edificio_table

create or replace TABLE PROPERTI.PROPERTI_SCHEMA.REFINED_GEBAEUDE_BATIMENT_EDIFICIO_TABLE (
	EGID ,
	GDEKT ,
	GGDENR ,
	GGDENAME ,
	EGRID ,
	LGBKR ,
	LPARZ ,
	LPARZSX ,
	LTYP ,
	GEBNR ,
	GBEZ ,
	GKODE ,
	GKODN ,
	GKSCE ,
	GSTAT ,
	GKAT ,
	GKLAS ,
	GBAUJ ,
	GBAUM ,
	GBAUP ,
	GABBJ ,
	GAREA ,

	GVOL ,
	GVOLNORM ,
	GVOLSCE ,
	GASTW ,
	GANZWHG ,

	GAZZI ,
	GSCHUTZR ,
	GEBF ,
	GWAERZH1 ,
	GENH1 ,
	GWAERSCEH1 ,
	GWAERDATH1 DATE,
	GWAERZH2 ,
	GENH2 ,
	GWAERSCEH2 ,
	GWAERDATH2 DATE,
	GWAERZW1 ,
	GENW1 ,
	GWAERSCEW1 ,
	GWAERDATW1 DATE,
	GWAERZW2 ,
	GENW2 ,
	GWAERSCEW2 ,
	GWAERDATW2 DATE,
	GEXPDAT DATE
);

-- Create raw_wohnung_logement_abitazione_table
create table raw_wohnung_logement_abitazione
(
    EGID         varchar,
    EWID         varchar,
    EDID         varchar,
    WHGNR        varchar,
    WEINR        varchar,
    WSTWK        varchar,
    WBEZ         varchar,
    WMEHRG       varchar,
    WBAUJ        varchar,
    WABBJ        varchar,
    WSTAT        varchar,
    WAREA        varchar,
    WAZIM        varchar,
    WKCHEWEXPDAT varchar
);

-- Create refined_wohnung_logement_abitazione_table
create or replace TABLE PROPERTI.PROPERTI_SCHEMA.REFINED_WOHNUNG_LOGEMENT_ABITAZIONE (
	EGID NUMBER(38,0),
	EWID NUMBER(38,0),
	EDID NUMBER(38,0),
	WHGNR VARCHAR(16777216),
	WEINR VARCHAR(16777216),
	WSTWK VARCHAR(16777216),
	WBEZ VARCHAR(16777216),
	WMEHRG VARCHAR(16777216),
	WBAUJ VARCHAR(16777216),
	WABBJ VARCHAR(16777216),
	WSTAT VARCHAR(16777216),
	WAREA VARCHAR(16777216),
	WAZIM VARCHAR(16777216),
	WKCHE VARCHAR(16777216),
	WEXPDAT DATE
);

-- Create data_pipeline_audit_table

create or replace TABLE data_pipeline_audit_table 
(unique_id varchar,
start_time datetime,
step varchar,
status varchar,
end_time datetime);