--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-27 17:25:04
--********************************************************************--
-- set odps.stage.mapper.split.size = 1024;
drop table if exists zhaopin_round2_ts ;

create table if not exists zhaopin_round2_ts as
select  p1.user_id
        ,p1.jd_no
        ,p2.kfold_flag
        ,label_2classes_satisfied
        ,label_3classes
        ---------------------------------------user_feature
        ,cur_city_id
        ,cur_industry_id
        ,cur_jd_type_id
        ,age
        ,user_work_years
        ,desire_jd_salary_id
        ,cur_salary_id
        ,cur_degree_id
        ---------------------------------------jd_feature
        ,p3.jd_city_id
        ,p3.jd_type_id
        ,require_nums
        ,p3.avg_salary
        ,is_travel
        ,p3.min_edu_level
        --
        ,jd_last_days
        ,jd_publish_days
        ,avg(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_avgbyuser
        ,max(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_maxbyuser
        ,min(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_minbyuser
        ,stddev(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_stdbyuser
        ,rank() over(partition by p1.user_id order by jd_publish_days ) as jd_publish_days_rankbyuser
        ,percent_rank()over(partition by p1.user_id order by jd_publish_days ) as jd_publish_days_rankbyuser_percentage
        ,jd_publish_days - avg(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_deltabyuseravg
        ,jd_publish_days - max(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_deltabyusermax
        ,jd_publish_days - min(jd_publish_days) over(partition by p1.user_id ) as jd_publish_days_deltabyusermin
        ,jd_end_days
        ,jd_begin_day
        ,jd_begin_month
        ,p3.min_years
        ,if(age!=-1 and age_1!=-1,age-age_1,-1) as agemin_delta
        ,if(age!=-1 and age_2!=-1,age-age_2,-1) as agemax_delta
        ,case   when age =-1 or (age_1 =-1 and age_2=-1) then -1
                when age_2 = -1 and age>=age_1 then 1
                when age_1 = -1 and age<=age_2 then 1
                when age>=age_1 and age<=age_2 then 1
                else 0 end as is_age_suit
        ,age_1
        ,age_2
        ,job_description_length
        ,sam_job_description_cnt
        ,job_description_letter_cnt
        ,job_description_letter_ratio
        ,job_description_number_cnt
        ,job_description_number_ratio
        ,jd_title_length
        ,jd_title_letter_cnt
        ,jd_title_letter_ratio
        ,jd_title_number_cnt
        ,jd_title_number_ratio
        ,requirement_cnt
        --------------------------------------- pv feature
        ,user_pv_cnt
        ,jd_pv_cnt
        ,user_jd_pair_pv_cnt
        ,user_pv_jd_cnt_unique
        ,jd_pv_user_cnt_unique
        ,user_jd_pair_pv_cnt_avgbyuser
        ,user_jd_pair_pv_cnt_maxbyuser
        ,user_jd_pair_pv_cnt_minbyuser
        ,user_jd_pair_pv_cnt_stdbyuser
        ,user_jd_pair_pv_cnt_rankbyuser
        ,user_jd_pair_pv_cnt_rankbyuser_percentage
        ,user_jd_pair_pv_cnt_deltabyuseravg
        ,user_jd_pair_pv_cnt_deltabyusermax
        ,user_jd_pair_pv_cnt_deltabyusermin
        ,jd_pv_user_cnt_unique_avgbyuser
        ,jd_pv_user_cnt_unique_maxbyuser
        ,jd_pv_user_cnt_unique_minbyuser
        ,jd_pv_user_cnt_unique_stdbyuser
        ,jd_pv_user_cnt_unique_rankbyuser
        ,jd_pv_user_cnt_unique_rankbyuser_percentage
        ,jd_pv_user_cnt_unique_deltabyuseravg
        ,jd_pv_user_cnt_unique_deltabyusermax
        ,jd_pv_user_cnt_unique_deltabyusermin
        ----------------------------------------jd_city_id
        --jd_city_id
        ,city_total_c1
        ,city_browsed_c1
        ,city_delivered_c1
        ,city_satisfied_c1
        ,city_browsed_r1
        ,city_delivered_r1
        ,city_satisfied_r1
        ,city_satisfied_r1_avgbyuser
        ,city_satisfied_r1_maxbyuser
        ,city_satisfied_r1_minbyuser
        ,city_satisfied_r1_stdbyuser
        ,city_satisfied_r1_rankbyuser
        ,city_satisfied_r1_rankbyuser_percentage
        ,city_satisfied_r1_deltabyuseravg
        ,city_satisfied_r1_deltabyusermax
        ,city_satisfied_r1_deltabyusermin
        --cur_city_id & jd_city_id
        ,city_total_c2
        ,city_browsed_c2
        ,city_delivered_c2
        ,city_satisfied_c2
        ,city_browsed_r2
        ,city_delivered_r2
        ,city_satisfied_r2
        ,city_satisfied_r2_avgbyuser
        ,city_satisfied_r2_maxbyuser
        ,city_satisfied_r2_minbyuser
        ,city_satisfied_r2_stdbyuser
        ,city_satisfied_r2_rankbyuser
        ,city_satisfied_r2_rankbyuser_percentage
        ,city_satisfied_r2_deltabyuseravg
        ,city_satisfied_r2_deltabyusermax
        ,city_satisfied_r2_deltabyusermin
        -----desire_jd_city_id & jd_city_id
        ,city_browsed_r3
        ,city_delivered_r3
        ,city_satisfied_r3
        ,city_satisfied_r3_avgbyuser
        ,city_satisfied_r3_stdbyuser
        ---------------------------------------jd_type_id
        --jd_type_id
        ,type_total_c1
        ,type_browsed_c1
        ,type_delivered_c1
        ,type_satisfied_c1
        ,type_browsed_r1
        ,type_delivered_r1
        ,type_satisfied_r1
        ,type_satisfied_r1_avgbyuser
        ,type_satisfied_r1_maxbyuser
        ,type_satisfied_r1_minbyuser
        ,type_satisfied_r1_stdbyuser
        ,type_satisfied_r1_rankbyuser
        ,type_satisfied_r1_rankbyuser_percentage
        ,type_satisfied_r1_deltabyuseravg
        ,type_satisfied_r1_deltabyusermax
        ,type_satisfied_r1_deltabyusermin
        --------cur_jd_type_id & jd_type_id
        ,type_total_c2
        ,type_browsed_c2
        ,type_delivered_c2
        ,type_satisfied_c2
        ,type_browsed_r2
        ,type_delivered_r2
        ,type_satisfied_r2
        ,type_satisfied_r2_avgbyuser
        ,type_satisfied_r2_maxbyuser
        ,type_satisfied_r2_minbyuser
        ,type_satisfied_r2_stdbyuser
        ,type_satisfied_r2_rankbyuser
        ,type_satisfied_r2_rankbyuser_percentage
        ,type_satisfied_r2_deltabyuseravg
        ,type_satisfied_r2_deltabyusermax
        ,type_satisfied_r2_deltabyusermin
        ------desire_jd_type_id & jd_type_id
        ,type_browsed_r3
        ,type_delivered_r3
        ,type_satisfied_r3
        ,type_satisfied_r3_avgbyuser
        ,type_satisfied_r3_stdbyuser
        --------------------------------------salary
        --desire_salary & avg_salary
        ,salary_total_c1
        ,salary_browsed_c1
        ,salary_delivered_c1
        ,salary_satisfied_c1
        ,salary_browsed_r1
        ,salary_delivered_r1
        ,salary_satisfied_r1
        ,salary_satisfied_r1_avgbyuser
        ,salary_satisfied_r1_maxbyuser
        ,salary_satisfied_r1_minbyuser
        ,salary_satisfied_r1_stdbyuser
        ,salary_satisfied_r1_rankbyuser
        ,salary_satisfied_r1_rankbyuser_percentage
        ,salary_satisfied_r1_deltabyuseravg
        ,salary_satisfied_r1_deltabyusermax
        ,salary_satisfied_r1_deltabyusermin
        --cur_salary & avg_salary
        ,salary_total_c2
        ,salary_browsed_c2
        ,salary_delivered_c2
        ,salary_satisfied_c2
        ,salary_browsed_r2
        ,salary_delivered_r2
        ,salary_satisfied_r2
        ,salary_satisfied_r2_avgbyuser
        ,salary_satisfied_r2_maxbyuser
        ,salary_satisfied_r2_minbyuser
        ,salary_satisfied_r2_stdbyuser
        ,salary_satisfied_r2_rankbyuser
        ,salary_satisfied_r2_rankbyuser_percentage
        ,salary_satisfied_r2_deltabyuseravg
        ,salary_satisfied_r2_deltabyusermax
        ,salary_satisfied_r2_deltabyusermin
        ------------------------------------min_years
        ,min_years_total_c2
        ,min_years_browsed_c2
        ,min_years_delivered_c2
        ,min_years_satisfied_c2
        ,min_years_browsed_r2
        ,min_years_delivered_r2
        ,min_years_satisfied_r2
        ,min_years_satisfied_r2_avgbyuser
        ,min_years_satisfied_r2_maxbyuser
        ,min_years_satisfied_r2_minbyuser
        ,min_years_satisfied_r2_stdbyuser
        ,min_years_satisfied_r2_rankbyuser
        ,min_years_satisfied_r2_rankbyuser_percentage
        ,min_years_satisfied_r2_deltabyuseravg
        ,min_years_satisfied_r2_deltabyusermax
        ,min_years_satisfied_r2_deltabyusermin
        ----------------------------------min_edu_level
        ,min_edu_level_total_c2
        ,min_edu_level_browsed_c2
        ,min_edu_level_delivered_c2
        ,min_edu_level_satisfied_c2
        ,min_edu_level_browsed_r2
        ,min_edu_level_delivered_r2
        ,min_edu_level_satisfied_r2
        ,min_edu_level_satisfied_r2_avgbyuser
        ,min_edu_level_satisfied_r2_maxbyuser
        ,min_edu_level_satisfied_r2_minbyuser
        ,min_edu_level_satisfied_r2_stdbyuser
        ,min_edu_level_satisfied_r2_rankbyuser
        ,min_edu_level_satisfied_r2_rankbyuser_percentage
        ,min_edu_level_satisfied_r2_deltabyuseravg
        ,min_edu_level_satisfied_r2_deltabyusermax
        ,min_edu_level_satisfied_r2_deltabyusermin
        ---------------------------------- jd历史投递率、满意率等feature
        ,jd_total_c
        ,jd_browsed_c
        ,jd_delivered_c
        ,jd_satisfied_c
        ,jd_browsed_r
        ,jd_delivered_r
        ,jd_satisfied_r
        ,jd_browsed_statisfied_r
        ,jd_delivered_statisfied_r
        ,jd_browsed_r_avgbyuser
        ,jd_browsed_r_maxbyuser
        ,jd_browsed_r_minbyuser
        ,jd_browsed_r_stdbyuser
        ,jd_browsed_r_rankbyuser
        ,jd_browsed_r_rankbyuser_percentage
        ,jd_browsed_r_deltabyuseravg
        ,jd_browsed_r_deltabyusermax
        ,jd_browsed_r_deltabyusermin
        ,jd_delivered_r_avgbyuser
        ,jd_delivered_r_maxbyuser
        ,jd_delivered_r_minbyuser
        ,jd_delivered_r_stdbyuser
        ,jd_delivered_r_rankbyuser
        ,jd_delivered_r_rankbyuser_percentage
        ,jd_delivered_r_deltabyuseravg
        ,jd_delivered_r_deltabyusermax
        ,jd_delivered_r_deltabyusermin
        ,jd_satisfied_r_avgbyuser
        ,jd_satisfied_r_maxbyuser
        ,jd_satisfied_r_minbyuser
        ,jd_satisfied_r_stdbyuser
        ,jd_satisfied_r_rankbyuser
        ,jd_satisfied_r_rankbyuser_percentage
        ,jd_satisfied_r_deltabyuseravg
        ,jd_satisfied_r_deltabyusermax
        ,jd_satisfied_r_deltabyusermin

        ----------------------------------------cross_feature
        ------------------------------represent_pv
        --jd
        ,user_jd_city_count
        ,user_jd_city_ratio
        ,user_jd_type_count
        ,user_jd_type_ratio
        ,user_jd_salary_avg
        ,user_jd_edu_avg
        ,user_jd_minyears_avg
        ,jd_publish_days_avg
        --user
        ,jd_user_cur_city_id_count
        ,jd_user_cur_city_id_ratio
        ,jd_user_cur_industry_id_count
        ,jd_user_cur_industry_id_ratio
        ,jd_user_cur_jd_type_id_count
        ,jd_user_cur_jd_type_id_ratio
        ,jd_user_user_work_years_avg
        ,jd_user_user_work_years_avg_deltabyuseractual
        ,jd_user_desire_jd_salary_id_avg
        ,jd_user_desire_jd_salary_id_avg_deltabyuseractual
        ,jd_user_cur_salary_id_avg
        ,jd_user_cur_salary_id_avg_deltabyuseractual
        ,jd_user_cur_degree_id_avg
        ,jd_user_cur_degree_id_avg_deltabyuseractual
        ,jd_age_avg
        ,jd_user_age_avg_deltabyuseractual
        -----------------------represent_action
        ,jd_user_cur_city_id_count_browsed
        ,jd_user_cur_city_id_ratio_browsed
        ,jd_user_cur_industry_id_count_browsed
        ,jd_user_cur_industry_id_ratio_browsed
        ,jd_user_cur_jd_type_id_count_browsed
        ,jd_user_cur_jd_type_id_ratio_browsed
        ,jd_user_user_work_years_avg_browsed
        ,jd_user_desire_jd_salary_id_avg_browsed
        ,jd_user_cur_salary_id_avg_browsed
        ,jd_user_cur_degree_id_avg_browsed
        ,jd_user_age_avg_browsed
        --
        ,jd_user_user_work_years_avg_browsed_deltabyactual
        ,jd_user_desire_jd_salary_id_avg_browsed_deltabyactual
        ,jd_user_cur_salary_id_avg_browsed_deltabyactual
        ,jd_user_cur_degree_id_avg_browsed_deltabyactual
        ,jd_user_age_avg_browsed_deltabyactual
        --------
        ,jd_user_cur_city_id_count_delivered
        ,jd_user_cur_city_id_ratio_delivered
        ,jd_user_cur_industry_id_count_delivered
        ,jd_user_cur_industry_id_ratio_delivered
        ,jd_user_cur_jd_type_id_count_delivered
        ,jd_user_cur_jd_type_id_ratio_delivered
        ,jd_user_user_work_years_avg_delivered
        ,jd_user_desire_jd_salary_id_avg_delivered
        ,jd_user_cur_salary_id_avg_delivered
        ,jd_user_cur_degree_id_avg_delivered
        ,jd_user_age_avg_delivered
        --
        ,jd_user_user_work_years_avg_delivered_deltabyactual
        ,jd_user_desire_jd_salary_id_avg_delivered_deltabyactual
        ,jd_user_cur_salary_id_avg_delivered_deltabyactual
        ,jd_user_cur_degree_id_avg_delivered_deltabyactual
        ,jd_user_age_avg_delivered_deltabyactual
        ------
        ,jd_user_cur_city_id_count_satisfied
        ,jd_user_cur_city_id_ratio_satisfied
        ,jd_user_cur_industry_id_count_satisfied
        ,jd_user_cur_industry_id_ratio_satisfied
        ,jd_user_cur_jd_type_id_count_satisfied
        ,jd_user_cur_jd_type_id_ratio_satisfied
        ,jd_user_user_work_years_avg_satisfied
        ,jd_user_desire_jd_salary_id_avg_satisfied
        ,jd_user_cur_salary_id_avg_satisfied
        ,jd_user_cur_degree_id_avg_satisfied
        ,jd_user_age_avg_satisfied
        --
        ,jd_user_user_work_years_avg_satisfied_deltabyactual
        ,jd_user_desire_jd_salary_id_avg_satisfied_deltabyactual
        ,jd_user_cur_salary_id_avg_satisfied_deltabyactual
        ,jd_user_cur_degree_id_avg_satisfied_deltabyactual
        ,jd_user_age_avg_satisfied_deltabyactual
        ---------------------------------
        ,jd_similarity_ratio_11
        ,jd_similarity_ratio_12
        ,jd_similarity_ratio_13
        ,jd_similarity_ratio_21
        ,jd_similarity_ratio_22
        ,jd_similarity_ratio_23
        ,jd_similarity_ratio_31
        ,jd_similarity_ratio_32
        ,jd_similarity_ratio_33
        -- ----- --------------------------------job_des
        ,exp_des_browsed_r
        ,exp_des_browsed_r_max
        ,exp_des_browsed_r_std
        ,exp_des_delivered_r
        ,exp_des_delivered_r_max
        ,exp_des_delivered_r_std
        ,exp_des_satisfied_r
        ,exp_des_satisfied_r_max
        ,exp_des_satisfied_r_std

        ,exp_cnt
        ,exp_find_cnt
        ,exp_find_ratio
        ,exp_find_ratio_avgbyuser
        ,exp_find_ratio_maxbyuser
        ,exp_find_ratio_minbyuser
        ,exp_find_ratio_stdbyuser
        ,exp_find_ratio_rankbyuser
        ,exp_find_ratio_rankbyuser_percentage
        ,exp_find_ratio_deltabyuseravg
        ,exp_find_ratio_deltabyusermax
        ,exp_find_ratio_deltabyusermin

        ,industry_des_browsed_r
        ,industry_des_browsed_r_max
        ,industry_des_browsed_r_std
        ,industry_des_delivered_r
        ,industry_des_delivered_r_max
        ,industry_des_delivered_r_std
        ,industry_des_satisfied_r
        ,industry_des_satisfied_r_max
        ,industry_des_satisfied_r_std
        ,type_des_browsed_r
        ,type_des_browsed_r_max
        ,type_des_browsed_r_std
        ,type_des_delivered_r
        ,type_des_delivered_r_max
        ,type_des_delivered_r_std
        ,type_des_satisfied_r
        ,type_des_satisfied_r_max
        ,type_des_satisfied_r_std
        ,industry_type_browsed_r
        ,industry_type_browsed_r_max
        ,industry_type_browsed_r_std
        ,industry_type_delivered_r
        ,industry_type_delivered_r_max
        ,industry_type_delivered_r_std
        ,industry_type_satisfied_r
        ,industry_type_satisfied_r_max
        ,industry_type_satisfied_r_std

from    (
            select  user_id
                    ,jd_no
                    ,cast(max(label_2classes_satisfied) as bigint ) as label_2classes_satisfied
                    ,cast(max(label_3classes) as bigint ) as label_3classes
            from    (
                        select  user_id
                                ,jd_no
                                ,if(satisfied=1,1,0) as label_2classes_satisfied
                                ,case    when delivered=0 then 0
                                         when satisfied=0 then 1
                                         else 2
                                 end as label_3classes
                        from    zhaopin_round2_all_action
                    ) t1
            group by user_id
                     ,jd_no
        ) p1
join    zhaopin_round2_user_p p2
on      p1.user_id = p2.user_id
join    zhaopin_round2_jd_p p3
on      p1.jd_no = p3.jd_no
join    zhaopin_round2_pv_f p4
on      p1.user_id = p4.user_id
and     p1.jd_no = p4.jd_no
join    zhaopin_round2_city_by1 p5
on      p1.user_id = p5.user_id
and     p3.jd_city_id = p5.jd_city_id
join    zhaopin_round2_city_by2 p6
on      p1.user_id = p6.user_id
and     p3.jd_city_id = p6.jd_city_id
join    zhaopin_round2_type_by1 p7
on      p1.user_id = p7.user_id
and     p3.jd_type_id = p7.jd_type_id
join    zhaopin_round2_type_by2 p8
on      p1.user_id = p8.user_id
and     p3.jd_type_id = p8.jd_type_id
join    zhaopin_round2_salary_by1 p9
on      p1.user_id = p9.user_id
and     p3.avg_salary = p9.avg_salary
join    zhaopin_round2_salary_by2 p10
on      p1.user_id = p10.user_id
and     p3.avg_salary = p10.avg_salary
join    zhaopin_round2_min_years_by2 p11
on      p1.user_id = p11.user_id
and     p3.min_years = p11.min_years
join    zhaopin_round2_min_edu_level_by2 p12
on      p1.user_id = p12.user_id
and     p3.min_edu_level = p12.min_edu_level
join    zhaopin_round2_jd_by p13
on      p1.user_id = p13.user_id
and     p1.jd_no = p13.jd_no
join    zhaopin_round2_represent_pv p14
on      p1.user_id = p14.user_id
and     p1.jd_no = p14.jd_no
join    zhaopin_round2_represent_action_user p15
on      p1.user_id = p15.user_id
and     p1.jd_no = p15.jd_no
join    zhaopin_round2_represent_similar_jd p16
on      p1.user_id = p16.user_id
and     p1.jd_no = p16.jd_no
join    zhaopin_round2_city_by3 p17
on      p1.user_id = p17.user_id
and     p3.jd_city_id = p17.jd_city_id
join    zhaopin_round2_type_by3 p18
on      p1.user_id = p18.user_id
and     p3.jd_type_id = p18.jd_type_id
-- join    zhaopin_round2_jd_publishdays_f p19
-- on      p1.user_id = p19.user_id
-- and     p1.jd_no = p19.jd_no
-- join    zhaopin_round2_all_pre_f p20
-- on      p1.user_id = p20.user_id
-- and     p1.jd_no = p20.jd_no
join    zhaopin_round2_exp_des_similarity p21
on      p1.user_id = p21.user_id
and     p1.jd_no = p21.jd_no
join    zhaopin_round2_exp_des_similarity2 p22
on      p1.user_id = p22.user_id
and     p1.jd_no = p22.jd_no
join    zhaopin_round2_industry_des_similarity p23
on      p1.user_id = p23.user_id
and     p1.jd_no = p23.jd_no
join    zhaopin_round2_type_des_similarity p24
on      p1.user_id = p24.user_id
and     p1.jd_no = p24.jd_no
join    zhaopin_round2_industry_type_similarity p25
on      p1.user_id = p25.user_id
and     p1.jd_no = p25.jd_no
;

--------------根据kfold_flag 划分训练集和测试集 这里采用了10折，跟5折区别不大。
--test set
drop table if exists zhaopin_round2_ts_t ;

create table if not exists zhaopin_round2_ts_t as
select  *
from    zhaopin_round2_ts
where   kfold_flag =  - 1
;
--train_fold 0

drop table if exists zhaopin_round2_ts_0 ;

create table if not exists zhaopin_round2_ts_0 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 0
;

drop table if exists zhaopin_round2_val_0 ;

create table if not exists zhaopin_round2_val_0 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 0
;
--train_fold 1

drop table if exists zhaopin_round2_ts_1 ;

create table if not exists zhaopin_round2_ts_1 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 1
;

drop table if exists zhaopin_round2_val_1 ;

create table if not exists zhaopin_round2_val_1 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 1
;
--train_fold 2

drop table if exists zhaopin_round2_ts_2 ;

create table if not exists zhaopin_round2_ts_2 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 2
;

drop table if exists zhaopin_round2_val_2 ;

create table if not exists zhaopin_round2_val_2 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 2
;
--train_fold 3

drop table if exists zhaopin_round2_ts_3 ;

create table if not exists zhaopin_round2_ts_3 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 3
;

drop table if exists zhaopin_round2_val_3 ;

create table if not exists zhaopin_round2_val_3 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 3
;
--train_fold 4

drop table if exists zhaopin_round2_ts_4 ;

create table if not exists zhaopin_round2_ts_4 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 4
;

drop table if exists zhaopin_round2_val_4 ;

create table if not exists zhaopin_round2_val_4 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 4
;
--train_fold 5

drop table if exists zhaopin_round2_ts_5 ;

create table if not exists zhaopin_round2_ts_5 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 5
;

drop table if exists zhaopin_round2_val_5 ;

create table if not exists zhaopin_round2_val_5 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 5
;
--train_fold 6

drop table if exists zhaopin_round2_ts_6 ;

create table if not exists zhaopin_round2_ts_6 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 6
;

drop table if exists zhaopin_round2_val_6 ;

create table if not exists zhaopin_round2_val_6 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 6
;
--train_fold 7

drop table if exists zhaopin_round2_ts_7 ;

create table if not exists zhaopin_round2_ts_7 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 7
;

drop table if exists zhaopin_round2_val_7 ;

create table if not exists zhaopin_round2_val_7 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 7
;
--train_fold 8

drop table if exists zhaopin_round2_ts_8 ;

create table if not exists zhaopin_round2_ts_8 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 8
;

drop table if exists zhaopin_round2_val_8 ;

create table if not exists zhaopin_round2_val_8 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 8
;
--train_fold 9

drop table if exists zhaopin_round2_ts_9 ;

create table if not exists zhaopin_round2_ts_9 as
select  *
from    zhaopin_round2_ts
where   kfold_flag !=  - 1
and     kfold_flag != 9
;

drop table if exists zhaopin_round2_val_9 ;

create table if not exists zhaopin_round2_val_9 as
select  *
from    zhaopin_round2_ts
where   kfold_flag = 9
;

---------------------------------------------


-- 有无特征选择，成绩区别不大，训练速度会快一点。
-- 特征选择 ps二分类 三分类筛选各top100特征，
-- drop table if exists zhaopin_round2_ts ;

-- create table if not exists zhaopin_round2_ts as
-- select  user_id
--         ,jd_no
--         ,kfold_flag
--         ,label_2classes_satisfied
--         ,label_3classes
--         ------------
--         ,user_jd_minyears_avg
--         ,exp_des_delivered_r
--         ,jd_browsed_r_rankbyuser
--         ,jd_similarity_ratio_21
--         ,min_edu_level_satisfied_r2_stdbyuser
--         ,type_satisfied_r1_deltabyusermax
--         ,jd_browsed_r_stdbyuser
--         ,jd_publish_days
--         ,jd_user_user_work_years_avg_satisfied_deltabyactual
--         ,jd_delivered_statisfied_r
--         ,jd_satisfied_r_stdbyuser
--         ,jd_satisfied_r_maxbyuser
--         ,user_jd_type_ratio
--         ,type_satisfied_r1_rankbyuser
--         ,is_age_suit
--         ,jd_browsed_statisfied_r
--         ,job_description_number_cnt
--         ,salary_delivered_r1
--         ,min_years_satisfied_r2_deltabyusermin
--         ,requirement_cnt
--         ,cur_industry_id
--         ,type_delivered_c1
--         ,min_years_satisfied_r2_deltabyusermax
--         ,type_satisfied_r3_avgbyuser
--         ,jd_satisfied_r_rankbyuser
--         ,user_jd_pair_pv_cnt_stdbyuser
--         ,min_edu_level_delivered_r2
--         ,city_browsed_r1
--         ,jd_end_days
--         ,city_satisfied_r3_stdbyuser
--         ,min_edu_level
--         ,min_edu_level_satisfied_r2_minbyuser
--         ,jd_satisfied_r_avgbyuser
--         ,exp_find_cnt
--         ,jd_user_user_work_years_avg_satisfied
--         ,jd_satisfied_r_deltabyusermax
--         ,exp_find_ratio_deltabyuseravg
--         ,type_delivered_r1
--         ,industry_des_delivered_r
--         ,type_delivered_r3
--         ,exp_find_ratio_maxbyuser
--         ,job_description_number_ratio
--         ,min_edu_level_browsed_r2
--         ,jd_similarity_ratio_23
--         ,user_jd_pair_pv_cnt_avgbyuser
--         ,jd_user_cur_city_id_ratio_satisfied
--         ,user_pv_jd_cnt_unique
--         ,jd_user_age_avg_satisfied_deltabyactual
--         ,jd_pv_user_cnt_unique_deltabyusermin
--         ,user_jd_salary_avg
--         ,type_total_c1
--         ,exp_find_ratio_rankbyuser_percentage
--         ,user_jd_city_count
--         ,jd_user_cur_city_id_count_satisfied
--         ,exp_find_ratio_minbyuser
--         ,jd_publish_days_rankbyuser
--         ,min_years_satisfied_r2_avgbyuser
--         ,jd_browsed_c
--         ,city_satisfied_c1
--         ,jd_pv_user_cnt_unique_deltabyuseravg
--         ,jd_satisfied_r_rankbyuser_percentage
--         ,salary_satisfied_r1
--         ,city_satisfied_r1_minbyuser
--         ,jd_pv_user_cnt_unique_stdbyuser
--         ,type_satisfied_r3_stdbyuser
--         ,jd_satisfied_c
--         ,job_description_length
--         ,jd_publish_days_rankbyuser_percentage
--         ,exp_find_ratio_avgbyuser
--         ,jd_delivered_r_stdbyuser
--         ,type_satisfied_c1
--         ,min_years_satisfied_r2_deltabyuseravg
--         ,jd_publish_days_avgbyuser
--         ,type_des_delivered_r
--         ,jd_satisfied_r_deltabyuseravg
--         ,city_browsed_c1
--         ,min_edu_level_satisfied_r2
--         ,jd_total_c
--         ,min_edu_level_satisfied_r2_deltabyusermax
--         ,industry_des_browsed_r
--         ,type_satisfied_r2_stdbyuser
--         ,type_satisfied_r2_avgbyuser
--         ,jd_user_cur_degree_id_avg_delivered
--         ,exp_find_ratio_stdbyuser
--         ,type_delivered_r2
--         ,jd_delivered_r_deltabyusermin
--         ,type_satisfied_r3
--         ,city_satisfied_r1_stdbyuser
--         ,is_travel
--         ,salary_satisfied_r1_deltabyuseravg
--         ,jd_user_cur_salary_id_avg_satisfied
--         ,jd_delivered_r_avgbyuser
--         ,type_browsed_r2
--         ,exp_find_ratio_rankbyuser
--         ,min_years_satisfied_r2_rankbyuser
--         ,city_total_c1
--         ,user_jd_type_count
--         ,jd_browsed_r_avgbyuser
--         ,exp_cnt
--         ,exp_des_satisfied_r
--         ,type_browsed_r1
--         ,type_satisfied_r2_rankbyuser
--         ,user_jd_pair_pv_cnt_deltabyusermax
--         ,type_browsed_r3
--         ,salary_satisfied_r1_stdbyuser
--         ,jd_satisfied_r_deltabyusermin
--         ,city_delivered_c1
--         ,type_satisfied_r2_minbyuser
--         ,city_browsed_r2
--         ,jd_pv_user_cnt_unique_rankbyuser
--         ,user_jd_pair_pv_cnt_deltabyuseravg
--         ,user_jd_pair_pv_cnt_rankbyuser
--         ,min_edu_level_satisfied_r2_deltabyuseravg
--         ,age
--         ,type_des_browsed_r
--         ,user_jd_edu_avg
--         ,jd_publish_days_deltabyusermin
--         ,type_des_satisfied_r
--         ,min_edu_level_satisfied_r2_rankbyuser
--         ,city_satisfied_r2_avgbyuser
--         ,jd_pv_user_cnt_unique_rankbyuser_percentage
--         ,type_satisfied_r1_minbyuser
--         ,require_nums
--         ,jd_pv_user_cnt_unique_deltabyusermax
--         ,salary_satisfied_r1_rankbyuser_percentage
--         ,jd_browsed_r
--         ,jd_delivered_r_rankbyuser_percentage
--         ,user_jd_city_ratio
--         ,city_satisfied_r1_deltabyusermin
--         ,avg_salary
--         ,city_delivered_r1
--         ,jd_user_age_avg_deltabyuseractual
--         ,jd_pv_cnt
--         ,city_browsed_r3
--         ,jd_satisfied_r
--         ,type_satisfied_r2_maxbyuser
--         ,jd_pv_user_cnt_unique
--         ,salary_satisfied_r1_rankbyuser
--         ,jd_pv_user_cnt_unique_minbyuser
--         ,city_delivered_r3
--         ,type_satisfied_r1_stdbyuser
--         ,user_jd_pair_pv_cnt_rankbyuser_percentage
--         ,city_delivered_r2
--         ,sam_job_description_cnt
--         ,city_satisfied_r2_minbyuser
--         ,jd_user_cur_city_id_count
--         ,type_satisfied_r1_avgbyuser
--         ,user_pv_cnt
--         ,industry_des_satisfied_r
--         ,exp_des_browsed_r
--         ,jd_user_cur_city_id_ratio_delivered
--         ,salary_satisfied_r2_stdbyuser
--         ,salary_satisfied_r2_minbyuser
--         ,jd_publish_days_stdbyuser
--         ,jd_delivered_c
--         ,jd_delivered_r
--         ,jd_user_age_avg_delivered
--         ,user_jd_pair_pv_cnt_maxbyuser
-- from    zhaopin_round2_ts_raw
-- ;