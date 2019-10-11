--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-09-08 17:25:04
--********************************************************************--
-- set odps.stage.mapper.split.size = 1024;

drop table if exists zhaopin_round2_ps_smart_3classes_pred_fea ;

create table if not exists zhaopin_round2_ps_smart_3classes_pred_fea as
select  user_id
        ,jd_no
        ,avg(score1) as ps_3classes_score1
        ,avg(score2) as ps_3classes_score2
from    (
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre0
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre1
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre2
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre3
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre4
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre5
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre6
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre7
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre8
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_pre9
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val0
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val1
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val2
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val3
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val4
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val5
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val6
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val7
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val8
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as score1
                    ,coalesce(get_json_object(prediction_detail,'$.2'),0) as score2
            from    zhaopin_round2_ps_smart_3classes_val9
        ) p1
group by user_id
         ,jd_no
;



drop table if exists zhaopin_round2_ps_smart_2classes_pred_fea ;

create table if not exists zhaopin_round2_ps_smart_2classes_pred_fea as
select  user_id
        ,jd_no
        ,avg(score) as ps_2classes_score
from    (
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre0
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre1
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre2
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre3
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre4
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre5
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre6
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre7
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre8
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_pre9
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val0
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val1
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val2
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val3
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val4
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val5
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val6
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val7
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val8
            union all
            select  user_id
                    ,jd_no
                    ,coalesce(get_json_object(prediction_detail,'$.1'),0) as score
            from    zhaopin_round2_ps_smart_2classes_val9
        ) p1
group by user_id
         ,jd_no
;

drop table if exists zhaopin_round2_stage2_ts ;

create table if not exists zhaopin_round2_stage2_ts as
select  p1.*
        ,ps_3classes_score1
        ,ps_3classes_score2
        ,ps_2classes_score
        --统计特征
        ,avg(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_avgbyuser
        ,max(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_maxbyuser
        ,min(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_minbyuser
        ,stddev(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_stdbyuser
        ,rank() over(partition by p1.user_id order by ps_3classes_score1 desc ) as ps_3classes_score1_rankbyuser
        ,percent_rank()over(partition by p1.user_id order by ps_3classes_score1 ) as ps_3classes_score1_rankbyuser_percentage
        ,ps_3classes_score1 - avg(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_deltabyuseravg
        ,ps_3classes_score1 - max(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_deltabyusermax
        ,ps_3classes_score1 - min(ps_3classes_score1) over(partition by p1.user_id ) as ps_3classes_score1_deltabyusermin
        --
        ,avg(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_avgbyuser
        ,max(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_maxbyuser
        ,min(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_minbyuser
        ,stddev(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_stdbyuser
        ,rank() over(partition by p1.user_id order by ps_3classes_score2 desc ) as ps_3classes_score2_rankbyuser
        ,percent_rank()over(partition by p1.user_id order by ps_3classes_score2 ) as ps_3classes_score2_rankbyuser_percentage
        ,ps_3classes_score2 - avg(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_deltabyuseravg
        ,ps_3classes_score2 - max(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_deltabyusermax
        ,ps_3classes_score2 - min(ps_3classes_score2) over(partition by p1.user_id ) as ps_3classes_score2_deltabyusermin
        --
        ,avg(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_avgbyuser
        ,max(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_maxbyuser
        ,min(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_minbyuser
        ,stddev(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_stdbyuser
        ,rank() over(partition by p1.user_id order by ps_2classes_score desc ) as ps_2classes_score_rankbyuser
        ,percent_rank()over(partition by p1.user_id order by ps_2classes_score ) as ps_2classes_score_rankbyuser_percentage
        ,ps_2classes_score - avg(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_deltabyuseravg
        ,ps_2classes_score - max(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_deltabyusermax
        ,ps_2classes_score - min(ps_2classes_score) over(partition by p1.user_id ) as ps_2classes_score_deltabyusermin
from    zhaopin_round2_ts p1
join    zhaopin_round2_ps_smart_3classes_pred_fea p2
on      p1.user_id = p2.user_id
and     p1.jd_no = p2.jd_no
join    zhaopin_round2_ps_smart_2classes_pred_fea p3
on      p1.user_id = p3.user_id
and     p1.jd_no = p3.jd_no
;
--------------根据kfold_flag 划分训练集和测试集
--test set
drop table if exists zhaopin_round2_stage2_ts_t ;

create table if not exists zhaopin_round2_stage2_ts_t as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag =  - 1
;
--train_fold 0

drop table if exists zhaopin_round2_stage2_ts_0 ;

create table if not exists zhaopin_round2_stage2_ts_0 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 0
and     kfold_flag != 1
;

drop table if exists zhaopin_round2_stage2_val_0 ;

create table if not exists zhaopin_round2_stage2_val_0 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag = 0
or      kfold_flag = 1
;
--train_fold 1

drop table if exists zhaopin_round2_stage2_ts_1 ;

create table if not exists zhaopin_round2_stage2_ts_1 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 2
and     kfold_flag != 3
;

drop table if exists zhaopin_round2_stage2_val_1 ;

create table if not exists zhaopin_round2_stage2_val_1 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag = 2
or      kfold_flag = 3
;
--train_fold 2

drop table if exists zhaopin_round2_stage2_ts_2 ;

create table if not exists zhaopin_round2_stage2_ts_2 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 4
and     kfold_flag != 5
;

drop table if exists zhaopin_round2_stage2_val_2 ;

create table if not exists zhaopin_round2_stage2_val_2 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag = 4
or      kfold_flag = 5
;
--train_fold 3

drop table if exists zhaopin_round2_stage2_ts_3 ;

create table if not exists zhaopin_round2_stage2_ts_3 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 6
and     kfold_flag != 7
;

drop table if exists zhaopin_round2_stage2_val_3 ;

create table if not exists zhaopin_round2_stage2_val_3 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag = 6
or      kfold_flag = 7
;
--train_fold 4

drop table if exists zhaopin_round2_stage2_ts_4 ;

create table if not exists zhaopin_round2_stage2_ts_4 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 8
and     kfold_flag != 9
;

drop table if exists zhaopin_round2_stage2_val_4 ;

create table if not exists zhaopin_round2_stage2_val_4 as
select  *
from    zhaopin_round2_stage2_ts
where   kfold_flag = 8
or      kfold_flag = 9
;
