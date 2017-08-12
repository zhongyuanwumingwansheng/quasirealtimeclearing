-- Create table
create table BMS_STL_INFO
(
  mer_no           CHAR(15) not null,
  apptype_id       INTEGER not null,
  mapp_main        VARCHAR2(32) not null,
  credit_calc_type CHAR(2) not null,
  credit_calc_rate NUMBER(15,5) not null,
  credit_calc_amt  NUMBER(14,2) not null,
  credit_min_amt   NUMBER(14,2) not null,
  credit_max_amt   NUMBER(14,2) not null
);
-- Create/Recreate indexes 
create index IDX_BMS_STL_INFO on BMS_STL_INFO (MER_NO);


-- Create table
create table SYS_TXN_CODE_INFO
(
  txn_key      VARCHAR2(50) not null,
  txn_code     CHAR(3) not null,
  txn_des      VARCHAR2(50) not null,
  bms_txn_code CHAR(4) not null,
  sett_flg     CHAR(1) not null,
  dc_flg       INTEGER not null,
  txn_code_grp CHAR(2) not null,
  rcd_ver      INTEGER not null,
  add_datetime CHAR(14) not null,
  add_user_id  CHAR(10) not null,
  upd_datetime CHAR(14),
  upd_user_id  CHAR(10)
);
-- Create/Recreate primary, unique and foreign key constraints 
alter table SYS_TXN_CODE_INFO  add primary key (TXN_KEY) using index ;



-- Create table
create table SYS_GROUP_ITEM_INFO
(
  inst_id               CHAR(8) not null,
  bat_date              CHAR(8) not null,
  group_id              VARCHAR2(50) not null,
  group_type_id         VARCHAR2(8) not null,
  item                  VARCHAR2(2000) not null,
  become_effective_date CHAR(8) not null,
  lost_effective_date   CHAR(8) not null,
  rcd_ver               INTEGER not null,
  add_datetime          CHAR(14) not null,
  add_user_id           CHAR(10) not null,
  upd_datetime          CHAR(14),
  upd_user_id           CHAR(10)
);
-- Create/Recreate indexes 
create index IDX_SYS_GROUP_ITEM_INFO_CK1 on SYS_GROUP_ITEM_INFO (ITEM);



-- Create table
create table SYS_MAP_ITEM_INFO
(
  inst_id               CHAR(8) not null,
  bat_date              CHAR(8) not null,
  map_id                INTEGER not null,
  type_id               INTEGER not null,
  src_item              VARCHAR2(500) not null,
  map_result            VARCHAR2(500) not null,
  become_effective_date CHAR(8) not null,
  lost_effective_date   CHAR(8) not null,
  rcd_ver               INTEGER not null,
  add_datetime          CHAR(14) not null,
  add_user_id           CHAR(10) not null,
  upd_datetime          CHAR(14),
  upd_user_id           CHAR(10)
);
-- Create/Recreate indexes 
create index SYS_MAP_ITEM_INFO_IDX1 on SYS_MAP_ITEM_INFO (SRC_ITEM, TYPE_ID, INST_ID, BAT_DATE);




