--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-23 19:11:25
--********************************************************************--
create table zhaopin_round2_user as
select  *
from    odps_tc_257100_f673506e024.zhaopin_round2_user
;

create table zhaopin_round2_jd as
select  *
from    odps_tc_257100_f673506e024.zhaopin_round2_jd
;

create table zhaopin_round2_action_train as
select  *
from    odps_tc_257100_f673506e024.zhaopin_round2_action_train
;

create table zhaopin_round2_action_test as
select  *
from    odps_tc_257100_f673506e024.zhaopin_round2_action_test
;


--submit_table_name:zhaopin_round2_submit

