--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-29 18:10:24
--********************************************************************--
--all action
drop table if exists zhaopin_round2_all_action ;

create table if not exists zhaopin_round2_all_action as
select  p1.user_id
        ,jd_no
        ,kfold_flag
        ,cast(browsed as bigint ) browsed
        ,cast(delivered as bigint ) delivered
        ,cast(satisfied as bigint ) satisfied
from    zhaopin_round2_action_train p1
join    zhaopin_round2_user_p p2
on      p1.user_id = p2.user_id
union all
--两表user_id无重合
select  user_id
        ,jd_no
        ,-1 as kfold_flag
        ,null as browsed
        ,null as delivered
        ,null as satisfied
from    zhaopin_round2_action_test
;
--all action2 用于k折提取历史特征

drop table if exists zhaopin_round2_all_action2 ;

create table if not exists zhaopin_round2_all_action2 as
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,-1 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,0 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 0
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,1 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 1
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,2 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 2
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,3 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 3
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,4 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 4
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,5 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 5
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,6 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 6
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,7 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 7
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,8 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 8
group by user_id
         ,jd_no
union all
select  user_id
        ,jd_no
        ,max(browsed) browsed
        ,max(delivered) delivered
        ,max(satisfied) satisfied
        ,9 as kfold_flag
from    zhaopin_round2_all_action
where   kfold_flag !=  - 1
and     kfold_flag != 9
group by user_id
         ,jd_no
;


-------------------------------------------pv特征
drop table if exists zhaopin_round2_pv_f ;
create table if not exists zhaopin_round2_pv_f as
select  *
        --user_jd_pair_pv_cnt
        ,avg(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_avgbyuser
        ,max(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_maxbyuser
        ,min(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_minbyuser
        ,stddev(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_stdbyuser
        ,rank() over(partition by user_id order by user_jd_pair_pv_cnt desc) as user_jd_pair_pv_cnt_rankbyuser
        ,percent_rank()over(partition by user_id order by user_jd_pair_pv_cnt desc) as user_jd_pair_pv_cnt_rankbyuser_percentage
        ,user_jd_pair_pv_cnt - avg(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_deltabyuseravg
        ,user_jd_pair_pv_cnt - max(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_deltabyusermax
        ,user_jd_pair_pv_cnt - min(user_jd_pair_pv_cnt) over(partition by user_id ) as user_jd_pair_pv_cnt_deltabyusermin
        --jd_pv_user_cnt_unique
        ,avg(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_avgbyuser
        ,max(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_maxbyuser
        ,min(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_minbyuser
        ,stddev(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_stdbyuser
        ,rank() over(partition by user_id order by jd_pv_user_cnt_unique ) as jd_pv_user_cnt_unique_rankbyuser
        ,percent_rank()over(partition by user_id order by jd_pv_user_cnt_unique ) as jd_pv_user_cnt_unique_rankbyuser_percentage
        ,jd_pv_user_cnt_unique - avg(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_deltabyuseravg
        ,jd_pv_user_cnt_unique - max(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_deltabyusermax
        ,jd_pv_user_cnt_unique - min(jd_pv_user_cnt_unique) over(partition by user_id ) as jd_pv_user_cnt_unique_deltabyusermin
from    (
            select  distinct user_id
                    ,jd_no
                    ,user_pv_cnt
                    ,jd_pv_cnt
                    ,user_jd_pair_pv_cnt
                    ,user_pv_jd_cnt_unique
                    ,jd_pv_user_cnt_unique
            from    (
                        select  user_id
                                ,jd_no
                                ,count(*) over(partition by user_id) as user_pv_cnt
                                ,count(*) over(partition by jd_no) as jd_pv_cnt
                                ,count(*) over(partition by user_id,jd_no)as user_jd_pair_pv_cnt
                                ,count(distinct jd_no) over(partition by user_id) as user_pv_jd_cnt_unique
                                ,count(distinct user_id) over(partition by jd_no) as jd_pv_user_cnt_unique
                        from    zhaopin_round2_all_action
                    ) t1
        ) t2
;

---------------------------------------------city_id
--jd_city_id
drop table if exists zhaopin_round2_city_by1 ;

create table if not exists zhaopin_round2_city_by1 as
select  t1.user_id
        ,t1.jd_city_id
        ,coalesce(city_c,0) city_total_c1
        ,coalesce(city_browsed_c,0) city_browsed_c1
        ,coalesce(city_delivered_c,0) city_delivered_c1
        ,coalesce(city_satisfied_c,0) city_satisfied_c1
        ,coalesce(city_browsed_r,0) city_browsed_r1
        ,coalesce(city_delivered_r,0) city_delivered_r1
        ,coalesce(city_satisfied_r,0) city_satisfied_r1
        ,avg(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_avgbyuser
        ,max(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_maxbyuser
        ,min(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_minbyuser
        ,stddev(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_stdbyuser
        ,rank() over(partition by user_id order by coalesce(city_satisfied_r,0) desc) as city_satisfied_r1_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(city_satisfied_r,0) desc) as city_satisfied_r1_rankbyuser_percentage
        ,coalesce(city_satisfied_r,0) - avg(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_deltabyuseravg
        ,coalesce(city_satisfied_r,0) - max(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_deltabyusermax
        ,coalesce(city_satisfied_r,0) - min(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r1_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,jd_city_id
            from    (
                        select  distinct user_id
                                ,jd_no
                                ,kfold_flag
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            group by p1.user_id
                     ,kfold_flag
                     ,jd_city_id
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,jd_city_id
                            ,count(*) as city_c
                            ,sum(browsed) as city_browsed_c
                            ,sum(delivered) as city_delivered_c
                            ,sum(satisfied) as city_satisfied_c
                            ,avg(browsed) as city_browsed_r
                            ,avg(delivered) as city_delivered_r
                            ,avg(satisfied) as city_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    group by jd_city_id
                             ,p1.kfold_flag
                ) t2
on      t1.jd_city_id = t2.jd_city_id
and     t1.kfold_flag = t2.kfold_flag
;

--cur_city_id & jd_city_id
drop table if exists zhaopin_round2_city_by2 ;

create table if not exists zhaopin_round2_city_by2 as
select  t1.user_id
        ,t1.jd_city_id
        ,coalesce(city_c,0) city_total_c2
        ,coalesce(city_browsed_c,0) city_browsed_c2
        ,coalesce(city_delivered_c,0) city_delivered_c2
        ,coalesce(city_satisfied_c,0) city_satisfied_c2
        ,coalesce(city_browsed_r,0) city_browsed_r2
        ,coalesce(city_delivered_r,0) city_delivered_r2
        ,coalesce(city_satisfied_r,0) city_satisfied_r2
        ,avg(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_avgbyuser
        ,max(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_maxbyuser
        ,min(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_minbyuser
        ,stddev(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_stdbyuser
        ,rank() over(partition by user_id order by coalesce(city_satisfied_r,0) desc) as city_satisfied_r2_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(city_satisfied_r,0) desc) as city_satisfied_r2_rankbyuser_percentage
        ,coalesce(city_satisfied_r,0) - avg(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_deltabyuseravg
        ,coalesce(city_satisfied_r,0) - max(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_deltabyusermax
        ,coalesce(city_satisfied_r,0) - min(coalesce(city_satisfied_r,0)) over(partition by user_id ) as city_satisfied_r2_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,cur_city_id
                    ,jd_city_id
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,cur_city_id
                     ,jd_city_id
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,cur_city_id
                            ,jd_city_id
                            ,count(*) as city_c
                            ,sum(browsed) as city_browsed_c
                            ,sum(delivered) as city_delivered_c
                            ,sum(satisfied) as city_satisfied_c
                            ,avg(browsed) as city_browsed_r
                            ,avg(delivered) as city_delivered_r
                            ,avg(satisfied) as city_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by cur_city_id
                             ,jd_city_id
                             ,p1.kfold_flag
                ) t2
on      t1.cur_city_id = t2.cur_city_id
and     t1.jd_city_id = t2.jd_city_id
and     t1.kfold_flag = t2.kfold_flag
;

--desire_jd_city_id & jd_city_id

drop table if exists zhaopin_round2_city_by3 ;

create table if not exists zhaopin_round2_city_by3 as
select  t1.user_id
        ,t1.jd_city_id
        ,coalesce(max(city_browsed_r),0) city_browsed_r3
        ,coalesce(max(city_delivered_r),0) city_delivered_r3
        ,coalesce(max(city_satisfied_r),0) city_satisfied_r3
        ,coalesce(avg(city_satisfied_r),0) as city_satisfied_r3_avgbyuser
        ,coalesce(stddev(city_satisfied_r),0) as city_satisfied_r3_stdbyuser
from    (
           select  distinct user_id
                    ,kfold_flag
                    ,jd_city_id
                    ,desire_jd_city_id
            from    (
                        select  trans_array(
                                    3
                                    ,','
                                    ,p1.user_id
                                    ,kfold_flag
                                    ,jd_city_id
                                    ,desire_jd_city_id
                                ) as (user_id ,kfold_flag ,jd_city_id ,desire_jd_city_id)
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                    from    zhaopin_round2_all_action
                                ) p1
                        join    zhaopin_round2_jd_p p2
                        on      p1.jd_no = p2.jd_no
                        join    zhaopin_round2_user_p p3
                        on      p1.user_id = p3.user_id
                    ) p4
        ) t1
left outer join (
                    select  kfold_flag
                            ,desire_jd_city_id
                            ,jd_city_id
                            ,avg(browsed) as city_browsed_r
                            ,avg(delivered) as city_delivered_r
                            ,avg(satisfied) as city_satisfied_r
                    from    (
                                select  trans_array(
                                            5
                                            ,','
                                            ,p1.kfold_flag
                                            ,jd_city_id
                                            ,browsed
                                            ,delivered
                                            ,satisfied
                                            ,desire_jd_city_id
                                        ) as (kfold_flag ,jd_city_id,browsed ,delivered ,satisfied ,desire_jd_city_id)
                                from    zhaopin_round2_all_action2 p1
                                join    zhaopin_round2_jd_p p2
                                on      p1.jd_no = p2.jd_no
                                join    zhaopin_round2_user_p p3
                                on      p1.user_id = p3.user_id
                            ) p4
                    where   desire_jd_city_id not in ('','-')
                    group by desire_jd_city_id
                             ,jd_city_id
                             ,kfold_flag
                ) t2
on      t1.desire_jd_city_id = t2.desire_jd_city_id
and     t1.jd_city_id = t2.jd_city_id
and     t1.kfold_flag = t2.kfold_flag
group by t1.user_id
         ,t1.jd_city_id
;
-----------------------------------jd_type

--jd_type_id
drop table if exists zhaopin_round2_type_by1 ;

create table if not exists zhaopin_round2_type_by1 as
select  t1.user_id
        ,t1.jd_type_id
        ,coalesce(type_c,0) type_total_c1
        ,coalesce(type_browsed_c,0) type_browsed_c1
        ,coalesce(type_delivered_c,0) type_delivered_c1
        ,coalesce(type_satisfied_c,0) type_satisfied_c1
        ,coalesce(type_browsed_r,0) type_browsed_r1
        ,coalesce(type_delivered_r,0) type_delivered_r1
        ,coalesce(type_satisfied_r,0) type_satisfied_r1
        ,avg(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_avgbyuser
        ,max(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_maxbyuser
        ,min(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_minbyuser
        ,stddev(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_stdbyuser
        ,rank() over(partition by user_id order by coalesce(type_satisfied_r,0) desc) as type_satisfied_r1_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(type_satisfied_r,0) desc) as type_satisfied_r1_rankbyuser_percentage
        ,coalesce(type_satisfied_r,0) - avg(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_deltabyuseravg
        ,coalesce(type_satisfied_r,0) - max(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_deltabyusermax
        ,coalesce(type_satisfied_r,0) - min(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r1_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,jd_type_id
            from    (
                        select  distinct user_id
                                ,jd_no
                                ,kfold_flag
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            group by p1.user_id
                     ,kfold_flag
                     ,jd_type_id
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,jd_type_id
                            ,count(*) as type_c
                            ,sum(browsed) as type_browsed_c
                            ,sum(delivered) as type_delivered_c
                            ,sum(satisfied) as type_satisfied_c
                            ,avg(browsed) as type_browsed_r
                            ,avg(delivered) as type_delivered_r
                            ,avg(satisfied) as type_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    group by jd_type_id
                             ,p1.kfold_flag
                ) t2
on      t1.jd_type_id = t2.jd_type_id
and     t1.kfold_flag = t2.kfold_flag
;


--cur_jd_type_id & jd_type_id
drop table if exists zhaopin_round2_type_by2 ;

create table if not exists zhaopin_round2_type_by2 as
select  t1.user_id
        ,t1.jd_type_id
        ,coalesce(type_c,0) type_total_c2
        ,coalesce(type_browsed_c,0) type_browsed_c2
        ,coalesce(type_delivered_c,0) type_delivered_c2
        ,coalesce(type_satisfied_c,0) type_satisfied_c2
        ,coalesce(type_browsed_r,0) type_browsed_r2
        ,coalesce(type_delivered_r,0) type_delivered_r2
        ,coalesce(type_satisfied_r,0) type_satisfied_r2
        ,avg(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_avgbyuser
        ,max(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_maxbyuser
        ,min(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_minbyuser
        ,stddev(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_stdbyuser
        ,rank() over(partition by user_id order by coalesce(type_satisfied_r,0) desc) as type_satisfied_r2_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(type_satisfied_r,0) desc) as type_satisfied_r2_rankbyuser_percentage
        ,coalesce(type_satisfied_r,0) - avg(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_deltabyuseravg
        ,coalesce(type_satisfied_r,0) - max(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_deltabyusermax
        ,coalesce(type_satisfied_r,0) - min(coalesce(type_satisfied_r,0)) over(partition by user_id ) as type_satisfied_r2_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,cur_jd_type_id
                    ,jd_type_id
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,cur_jd_type_id
                     ,jd_type_id
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,cur_jd_type_id
                            ,jd_type_id
                            ,count(*) as type_c
                            ,sum(browsed) as type_browsed_c
                            ,sum(delivered) as type_delivered_c
                            ,sum(satisfied) as type_satisfied_c
                            ,avg(browsed) as type_browsed_r
                            ,avg(delivered) as type_delivered_r
                            ,avg(satisfied) as type_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by cur_jd_type_id
                             ,jd_type_id
                             ,p1.kfold_flag
                ) t2
on      t1.cur_jd_type_id = t2.cur_jd_type_id
and     t1.jd_type_id = t2.jd_type_id
and     t1.kfold_flag = t2.kfold_flag
;

--desire_jd_type_id & jd_type_id
drop table if exists zhaopin_round2_type_by3 ;

create table if not exists zhaopin_round2_type_by3 as
select  t1.user_id
        ,t1.jd_type_id
        ,coalesce(max(type_browsed_r),0) type_browsed_r3
        ,coalesce(max(type_delivered_r),0) type_delivered_r3
        ,coalesce(max(type_satisfied_r),0) type_satisfied_r3
        ,coalesce(avg(type_satisfied_r),0) as type_satisfied_r3_avgbyuser
        ,coalesce(stddev(type_satisfied_r),0) as type_satisfied_r3_stdbyuser
from    (
            select  distinct user_id
                    ,kfold_flag
                    ,jd_type_id
                    ,desire_jd_type_id
            from    (
                        select  trans_array(
                                    3
                                    ,','
                                    ,p1.user_id
                                    ,kfold_flag
                                    ,jd_type_id
                                    ,desire_jd_type_id
                                ) as (user_id ,kfold_flag ,jd_type_id ,desire_jd_type_id)
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                    from    zhaopin_round2_all_action
                                ) p1
                        join    zhaopin_round2_jd_p p2
                        on      p1.jd_no = p2.jd_no
                        join    zhaopin_round2_user_p p3
                        on      p1.user_id = p3.user_id
                    ) p4
        ) t1
left outer join (
                    select  kfold_flag
                            ,desire_jd_type_id
                            ,jd_type_id
                            ,avg(browsed) as type_browsed_r
                            ,avg(delivered) as type_delivered_r
                            ,avg(satisfied) as type_satisfied_r
                    from    (
                                select  trans_array(
                                            5
                                            ,','
                                            ,p1.kfold_flag
                                            ,jd_type_id
                                            ,browsed
                                            ,delivered
                                            ,satisfied
                                            ,desire_jd_type_id
                                        ) as (kfold_flag ,jd_type_id,browsed ,delivered ,satisfied ,desire_jd_type_id)
                                from    zhaopin_round2_all_action2 p1
                                join    zhaopin_round2_jd_p p2
                                on      p1.jd_no = p2.jd_no
                                join    zhaopin_round2_user_p p3
                                on      p1.user_id = p3.user_id
                            ) p4
                    where   desire_jd_type_id != ''
                    group by desire_jd_type_id
                             ,jd_type_id
                             ,kfold_flag
                ) t2
on      t1.desire_jd_type_id = t2.desire_jd_type_id
and     t1.jd_type_id = t2.jd_type_id
and     t1.kfold_flag = t2.kfold_flag
group by t1.user_id
         ,t1.jd_type_id
;

--------------------------------------------------------salary
--desire_salary & jd_avg_salary

drop table if exists zhaopin_round2_salary_by1 ;

create table if not exists zhaopin_round2_salary_by1 as
select  t1.user_id
        ,t1.avg_salary
        ,coalesce(salary_c,0) salary_total_c1
        ,coalesce(salary_browsed_c,0) salary_browsed_c1
        ,coalesce(salary_delivered_c,0) salary_delivered_c1
        ,coalesce(salary_satisfied_c,0) salary_satisfied_c1
        ,coalesce(salary_browsed_r,0) salary_browsed_r1
        ,coalesce(salary_delivered_r,0) salary_delivered_r1
        ,coalesce(salary_satisfied_r,0) salary_satisfied_r1
        ,avg(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_avgbyuser
        ,max(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_maxbyuser
        ,min(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_minbyuser
        ,stddev(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_stdbyuser
        ,rank() over(partition by user_id order by coalesce(salary_satisfied_r,0) desc) as salary_satisfied_r1_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(salary_satisfied_r,0) desc) as salary_satisfied_r1_rankbyuser_percentage
        ,coalesce(salary_satisfied_r,0) - avg(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_deltabyuseravg
        ,coalesce(salary_satisfied_r,0) - max(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_deltabyusermax
        ,coalesce(salary_satisfied_r,0) - min(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r1_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,desire_jd_salary_id
                    ,avg_salary
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,desire_jd_salary_id
                     ,avg_salary
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,desire_jd_salary_id
                            ,avg_salary
                            ,count(*) as salary_c
                            ,sum(browsed) as salary_browsed_c
                            ,sum(delivered) as salary_delivered_c
                            ,sum(satisfied) as salary_satisfied_c
                            ,avg(browsed) as salary_browsed_r
                            ,avg(delivered) as salary_delivered_r
                            ,avg(satisfied) as salary_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by desire_jd_salary_id
                             ,avg_salary
                             ,p1.kfold_flag
                ) t2
on      t1.desire_jd_salary_id = t2.desire_jd_salary_id
and     t1.avg_salary = t2.avg_salary
and     t1.kfold_flag = t2.kfold_flag
;

--cur_salary & jd_avg_salary
drop table if exists zhaopin_round2_salary_by2 ;

create table if not exists zhaopin_round2_salary_by2 as
select  t1.user_id
        ,t1.avg_salary
        ,coalesce(salary_c,0) salary_total_c2
        ,coalesce(salary_browsed_c,0) salary_browsed_c2
        ,coalesce(salary_delivered_c,0) salary_delivered_c2
        ,coalesce(salary_satisfied_c,0) salary_satisfied_c2
        ,coalesce(salary_browsed_r,0) salary_browsed_r2
        ,coalesce(salary_delivered_r,0) salary_delivered_r2
        ,coalesce(salary_satisfied_r,0) salary_satisfied_r2
        ,avg(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_avgbyuser
        ,max(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_maxbyuser
        ,min(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_minbyuser
        ,stddev(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_stdbyuser
        ,rank() over(partition by user_id order by coalesce(salary_satisfied_r,0) desc) as salary_satisfied_r2_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(salary_satisfied_r,0) desc) as salary_satisfied_r2_rankbyuser_percentage
        ,coalesce(salary_satisfied_r,0) - avg(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_deltabyuseravg
        ,coalesce(salary_satisfied_r,0) - max(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_deltabyusermax
        ,coalesce(salary_satisfied_r,0) - min(coalesce(salary_satisfied_r,0)) over(partition by user_id ) as salary_satisfied_r2_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,cur_salary_id
                    ,avg_salary
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,cur_salary_id
                     ,avg_salary
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,cur_salary_id
                            ,avg_salary
                            ,count(*) as salary_c
                            ,sum(browsed) as salary_browsed_c
                            ,sum(delivered) as salary_delivered_c
                            ,sum(satisfied) as salary_satisfied_c
                            ,avg(browsed) as salary_browsed_r
                            ,avg(delivered) as salary_delivered_r
                            ,avg(satisfied) as salary_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by cur_salary_id
                             ,avg_salary
                             ,p1.kfold_flag
                ) t2
on      t1.cur_salary_id = t2.cur_salary_id
and     t1.avg_salary = t2.avg_salary
and     t1.kfold_flag = t2.kfold_flag
;

--------------------------------------------min_years
drop table if exists zhaopin_round2_min_years_by2 ;

create table if not exists zhaopin_round2_min_years_by2 as
select  t1.user_id
        ,t1.min_years
        ,coalesce(min_years_c,0) min_years_total_c2
        ,coalesce(min_years_browsed_c,0) min_years_browsed_c2
        ,coalesce(min_years_delivered_c,0) min_years_delivered_c2
        ,coalesce(min_years_satisfied_c,0) min_years_satisfied_c2
        ,coalesce(min_years_browsed_r,0) min_years_browsed_r2
        ,coalesce(min_years_delivered_r,0) min_years_delivered_r2
        ,coalesce(min_years_satisfied_r,0) min_years_satisfied_r2
        ,avg(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_avgbyuser
        ,max(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_maxbyuser
        ,min(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_minbyuser
        ,stddev(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_stdbyuser
        ,rank() over(partition by user_id order by coalesce(min_years_satisfied_r,0) desc) as min_years_satisfied_r2_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(min_years_satisfied_r,0) desc) as min_years_satisfied_r2_rankbyuser_percentage
        ,coalesce(min_years_satisfied_r,0) - avg(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_deltabyuseravg
        ,coalesce(min_years_satisfied_r,0) - max(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_deltabyusermax
        ,coalesce(min_years_satisfied_r,0) - min(coalesce(min_years_satisfied_r,0)) over(partition by user_id ) as min_years_satisfied_r2_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,user_work_years
                    ,min_years
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,user_work_years
                     ,min_years
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,user_work_years
                            ,min_years
                            ,count(*) as min_years_c
                            ,sum(browsed) as min_years_browsed_c
                            ,sum(delivered) as min_years_delivered_c
                            ,sum(satisfied) as min_years_satisfied_c
                            ,avg(browsed) as min_years_browsed_r
                            ,avg(delivered) as min_years_delivered_r
                            ,avg(satisfied) as min_years_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by user_work_years
                             ,min_years
                             ,p1.kfold_flag
                ) t2
on      t1.user_work_years = t2.user_work_years
and     t1.min_years = t2.min_years
and     t1.kfold_flag = t2.kfold_flag
;


-----------------------------------------------min_edu_level
drop table if exists zhaopin_round2_min_edu_level_by2 ;

create table if not exists zhaopin_round2_min_edu_level_by2 as
select  t1.user_id
        ,t1.min_edu_level
        ,coalesce(min_edu_level_c,0) min_edu_level_total_c2
        ,coalesce(min_edu_level_browsed_c,0) min_edu_level_browsed_c2
        ,coalesce(min_edu_level_delivered_c,0) min_edu_level_delivered_c2
        ,coalesce(min_edu_level_satisfied_c,0) min_edu_level_satisfied_c2
        ,coalesce(min_edu_level_browsed_r,0) min_edu_level_browsed_r2
        ,coalesce(min_edu_level_delivered_r,0) min_edu_level_delivered_r2
        ,coalesce(min_edu_level_satisfied_r,0) min_edu_level_satisfied_r2
        ,avg(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_avgbyuser
        ,max(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_maxbyuser
        ,min(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_minbyuser
        ,stddev(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_stdbyuser
        ,rank() over(partition by user_id order by coalesce(min_edu_level_satisfied_r,0) desc) as min_edu_level_satisfied_r2_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(min_edu_level_satisfied_r,0) desc) as min_edu_level_satisfied_r2_rankbyuser_percentage
        ,coalesce(min_edu_level_satisfied_r,0) - avg(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_deltabyuseravg
        ,coalesce(min_edu_level_satisfied_r,0) - max(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_deltabyusermax
        ,coalesce(min_edu_level_satisfied_r,0) - min(coalesce(min_edu_level_satisfied_r,0)) over(partition by user_id ) as min_edu_level_satisfied_r2_deltabyusermin
from    (
            select  p1.user_id
                    ,kfold_flag
                    ,cur_degree_id
                    ,min_edu_level
            from    (
                        select  distinct user_id
                                ,jd_no
                        from    zhaopin_round2_all_action
                    ) p1
            join    zhaopin_round2_jd_p p2
            on      p1.jd_no = p2.jd_no
            join    zhaopin_round2_user_p p3
            on      p1.user_id = p3.user_id
            group by p1.user_id
                     ,kfold_flag
                     ,cur_degree_id
                     ,min_edu_level
        ) t1
left outer join (
                    select  p1.kfold_flag
                            ,cur_degree_id
                            ,min_edu_level
                            ,count(*) as min_edu_level_c
                            ,sum(browsed) as min_edu_level_browsed_c
                            ,sum(delivered) as min_edu_level_delivered_c
                            ,sum(satisfied) as min_edu_level_satisfied_c
                            ,avg(browsed) as min_edu_level_browsed_r
                            ,avg(delivered) as min_edu_level_delivered_r
                            ,avg(satisfied) as min_edu_level_satisfied_r
                    from    zhaopin_round2_all_action2 p1
                    join    zhaopin_round2_jd_p p2
                    on      p1.jd_no = p2.jd_no
                    join    zhaopin_round2_user_p p3
                    on      p1.user_id = p3.user_id
                    group by cur_degree_id
                             ,min_edu_level
                             ,p1.kfold_flag
                ) t2
on      t1.cur_degree_id = t2.cur_degree_id
and     t1.min_edu_level = t2.min_edu_level
and     t1.kfold_flag = t2.kfold_flag
;


----------------------------
--jd的点击、投递、满意情况
drop table if exists zhaopin_round2_jd_by ;

create table if not exists zhaopin_round2_jd_by as
select  t1.user_id
        ,t1.jd_no
        ,coalesce(jd_total_c,0) as jd_total_c
        ,coalesce(jd_browsed_c,0) as jd_browsed_c
        ,coalesce(jd_delivered_c,0) as jd_delivered_c
        ,coalesce(jd_satisfied_c,0) as jd_satisfied_c
        ,coalesce(jd_browsed_r,0) as jd_browsed_r
        ,coalesce(jd_delivered_r,0) as jd_delivered_r
        ,coalesce(jd_satisfied_r,0) as jd_satisfied_r
        ,if(coalesce(jd_browsed_c,0)>0,coalesce(jd_satisfied_c,0)/coalesce(jd_browsed_c,0),0) as jd_browsed_statisfied_r
        ,if(coalesce(jd_delivered_c,0)>0,coalesce(jd_satisfied_c,0)/coalesce(jd_delivered_c,0),0) as jd_delivered_statisfied_r
        --
        ,avg(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_avgbyuser
        ,max(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_maxbyuser
        ,min(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_minbyuser
        ,stddev(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_stdbyuser
        ,rank() over(partition by user_id order by coalesce(jd_browsed_r,0) desc) as jd_browsed_r_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(jd_browsed_r,0) desc) as jd_browsed_r_rankbyuser_percentage
        ,coalesce(jd_browsed_r,0) - avg(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_deltabyuseravg
        ,coalesce(jd_browsed_r,0) - max(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_deltabyusermax
        ,coalesce(jd_browsed_r,0) - min(coalesce(jd_browsed_r,0)) over(partition by user_id ) as jd_browsed_r_deltabyusermin
        --
        ,avg(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_avgbyuser
        ,max(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_maxbyuser
        ,min(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_minbyuser
        ,stddev(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_stdbyuser
        ,rank() over(partition by user_id order by coalesce(jd_delivered_r,0) desc) as jd_delivered_r_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(jd_delivered_r,0) desc) as jd_delivered_r_rankbyuser_percentage
        ,coalesce(jd_delivered_r,0) - avg(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_deltabyuseravg
        ,coalesce(jd_delivered_r,0) - max(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_deltabyusermax
        ,coalesce(jd_delivered_r,0) - min(coalesce(jd_delivered_r,0)) over(partition by user_id ) as jd_delivered_r_deltabyusermin
        --
        ,avg(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_avgbyuser
        ,max(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_maxbyuser
        ,min(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_minbyuser
        ,stddev(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_stdbyuser
        ,rank() over(partition by user_id order by coalesce(jd_satisfied_r,0) desc) as jd_satisfied_r_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(jd_satisfied_r,0) desc) as jd_satisfied_r_rankbyuser_percentage
        ,coalesce(jd_satisfied_r,0) - avg(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_deltabyuseravg
        ,coalesce(jd_satisfied_r,0) - max(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_deltabyusermax
        ,coalesce(jd_satisfied_r,0) - min(coalesce(jd_satisfied_r,0)) over(partition by user_id ) as jd_satisfied_r_deltabyusermin
from    (
            select  distinct user_id
                    ,jd_no
                    ,kfold_flag
            from    zhaopin_round2_all_action
        ) t1
left outer join (
                    select  jd_no
                            ,kfold_flag
                            ,count(*) as jd_total_c
                            ,sum(browsed)as jd_browsed_c
                            ,sum(delivered)as jd_delivered_c
                            ,sum(satisfied)as jd_satisfied_c
                            ,avg(browsed) as jd_browsed_r
                            ,avg(delivered) as jd_delivered_r
                            ,avg(satisfied) as jd_satisfied_r
                    from    zhaopin_round2_all_action2
                    group by jd_no
                             ,kfold_flag
                ) t2
on      t1.jd_no = t2.jd_no
and     t1.kfold_flag = t2.kfold_flag
;

--jd 跟 该user曝光的jd集合差距& user跟jd的历史曝光user集合差距
drop table if exists zhaopin_round2_represent_pv ;

create table if not exists zhaopin_round2_represent_pv as
select  p1.user_id
        ,p1.jd_no
        --jd_fea
        ,count(if(jd_city_id!=-1,jd_city_id,null)) over(partition by p1.user_id,jd_city_id ) as user_jd_city_count
        ,case    when count(if(jd_city_id!=-1,jd_city_id,null)) over(partition by p1.user_id) = 0 then 0
                 else count(if(jd_city_id!=-1,jd_city_id,null)) over(partition by p1.user_id,jd_city_id )/count(if(jd_city_id!=-1,jd_city_id,null)) over(partition by p1.user_id)
         end as user_jd_city_ratio
        ,count(if(jd_type_id!=-1,jd_type_id,null)) over(partition by p1.user_id,jd_type_id ) as user_jd_type_count
        ,case    when count(if(jd_type_id!=-1,jd_type_id,null)) over(partition by p1.user_id) = 0 then 0
                 else count(if(jd_type_id!=-1,jd_type_id,null)) over(partition by p1.user_id,jd_type_id )/count(if(jd_type_id!=-1,jd_type_id,null)) over(partition by p1.user_id)
         end as user_jd_type_ratio
        ,coalesce(avg(if(avg_salary!=-1,avg_salary,null))over(partition by p1.user_id ),-1)  as user_jd_salary_avg
        ,coalesce(avg(if(min_edu_level!=-1,min_edu_level,null)) over(partition by p1.user_id),-1) as user_jd_edu_avg
        ,coalesce(avg(if(min_years!=-1,min_years,null)) over(partition by p1.user_id),-1) as user_jd_minyears_avg
        ,coalesce(avg(jd_publish_days) over(partition by p1.user_id),-1) as jd_publish_days_avg
        --user_fea
        ,count(if(cur_city_id!=-1,cur_city_id,null)) over(partition by p1.jd_no,cur_city_id ) as jd_user_cur_city_id_count
        ,case    when count(if(cur_city_id!=-1,cur_city_id,null)) over(partition by p1.jd_no) = 0 then 0
                 else count(if(cur_city_id!=-1,cur_city_id,null)) over(partition by p1.jd_no,cur_city_id )/count(if(cur_city_id!=-1,cur_city_id,null)) over(partition by p1.jd_no)
         end as jd_user_cur_city_id_ratio
        ,count(if(cur_industry_id!=-1,cur_industry_id,null)) over(partition by p1.jd_no,cur_industry_id ) as jd_user_cur_industry_id_count
        ,case    when count(if(cur_industry_id!=-1,cur_industry_id,null)) over(partition by p1.jd_no) = 0 then 0
                 else count(if(cur_industry_id!=-1,cur_industry_id,null)) over(partition by p1.jd_no,cur_industry_id )/count(if(cur_industry_id!=-1,cur_industry_id,null)) over(partition by p1.jd_no)
         end as jd_user_cur_industry_id_ratio
        ,count(if(cur_jd_type_id!=-1,cur_jd_type_id,null)) over(partition by p1.jd_no,cur_jd_type_id ) as jd_user_cur_jd_type_id_count
        ,case    when count(if(cur_jd_type_id!=-1,cur_jd_type_id,null)) over(partition by p1.jd_no) = 0 then 0
                 else count(if(cur_jd_type_id!=-1,cur_jd_type_id,null)) over(partition by p1.jd_no,cur_jd_type_id )/count(if(cur_jd_type_id!=-1,cur_jd_type_id,null)) over(partition by p1.jd_no)
         end as jd_user_cur_jd_type_id_ratio
        ,coalesce(avg(if(user_work_years!=-1,user_work_years,null)) over(partition by p1.jd_no),-1) as jd_user_user_work_years_avg
        ,case when coalesce(avg(if(user_work_years!=-1,user_work_years,null)) over(partition by p1.jd_no),-1)!=-1 then abs(user_work_years - coalesce(avg(if(user_work_years!=-1,user_work_years,null)) over(partition by p1.jd_no),-1))
            else -1 end as jd_user_user_work_years_avg_deltabyuseractual
        ,coalesce(avg(if(desire_jd_salary_id!=-1,desire_jd_salary_id,null)) over(partition by p1.jd_no),-1) as jd_user_desire_jd_salary_id_avg
        ,case when coalesce(avg(if(desire_jd_salary_id!=-1,desire_jd_salary_id,null)) over(partition by p1.jd_no),-1)!=-1 then abs(desire_jd_salary_id - coalesce(avg(if(desire_jd_salary_id!=-1,desire_jd_salary_id,null)) over(partition by p1.jd_no),-1))
            else -1 end as jd_user_desire_jd_salary_id_avg_deltabyuseractual
        ,coalesce(avg(if(cur_salary_id!=-1,cur_salary_id,null)) over(partition by p1.jd_no),-1) as jd_user_cur_salary_id_avg
        ,case when coalesce(avg(if(cur_salary_id!=-1,cur_salary_id,null)) over(partition by p1.jd_no),-1)!=-1 then abs(cur_salary_id - coalesce(avg(if(cur_salary_id!=-1,cur_salary_id,null)) over(partition by p1.jd_no),-1))
            else -1 end as jd_user_cur_salary_id_avg_deltabyuseractual
        ,coalesce(avg(if(cur_degree_id!=-1,cur_degree_id,null)) over(partition by p1.jd_no ),-1) as jd_user_cur_degree_id_avg
        ,case when coalesce(avg(if(cur_degree_id!=-1,cur_degree_id,null)) over(partition by p1.jd_no),-1)!=-1 then abs(cur_degree_id - coalesce(avg(if(cur_degree_id!=-1,cur_degree_id,null)) over(partition by p1.jd_no),-1))
            else -1 end as jd_user_cur_degree_id_avg_deltabyuseractual
        ,coalesce(avg(if(age!=-1,age,null)) over(partition by p1.jd_no) ,-1)as jd_age_avg
        ,case when coalesce(avg(if(age!=-1,age,null)) over(partition by p1.jd_no),-1)!=-1 then abs(age - coalesce(avg(if(age!=-1,age,null)) over(partition by p1.jd_no),-1))
            else -1 end as jd_user_age_avg_deltabyuseractual
from    (
            select  distinct user_id
                    ,jd_no
            from    zhaopin_round2_all_action
        ) p1
join    zhaopin_round2_jd_p p2
on      p1.jd_no = p2.jd_no
join    zhaopin_round2_user_p p3
on      p1.user_id = p3.user_id
;

--user 跟jd的历史user集合（分点击、投递、满意三种情况）差距
drop table if exists zhaopin_round2_represent_action_user ;

create table if not exists zhaopin_round2_represent_action_user as
select  p1.user_id
        ,jd_no
        ----
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
        ,if(jd_user_user_work_years_avg_browsed != -1 and user_work_years != -1,abs(user_work_years-jd_user_user_work_years_avg_browsed),-1) jd_user_user_work_years_avg_browsed_deltabyactual
        ,if(jd_user_desire_jd_salary_id_avg_browsed != -1 and desire_jd_salary_id != -1,abs(desire_jd_salary_id-jd_user_desire_jd_salary_id_avg_browsed),-1) jd_user_desire_jd_salary_id_avg_browsed_deltabyactual
        ,if(jd_user_cur_salary_id_avg_browsed != -1 and cur_salary_id != -1,abs(cur_salary_id-jd_user_cur_salary_id_avg_browsed),-1) jd_user_cur_salary_id_avg_browsed_deltabyactual
        ,if(jd_user_cur_degree_id_avg_browsed != -1 and cur_degree_id != -1,abs(cur_degree_id-jd_user_cur_degree_id_avg_browsed),-1) jd_user_cur_degree_id_avg_browsed_deltabyactual
        ,if(jd_user_age_avg_browsed != -1 and age != -1,abs(age-jd_user_age_avg_browsed),-1) jd_user_age_avg_browsed_deltabyactual
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
        ,if(jd_user_user_work_years_avg_delivered != -1 and user_work_years != -1,abs(user_work_years-jd_user_user_work_years_avg_delivered),-1) jd_user_user_work_years_avg_delivered_deltabyactual
        ,if(jd_user_desire_jd_salary_id_avg_delivered != -1 and desire_jd_salary_id != -1,abs(desire_jd_salary_id-jd_user_desire_jd_salary_id_avg_delivered),-1) jd_user_desire_jd_salary_id_avg_delivered_deltabyactual
        ,if(jd_user_cur_salary_id_avg_delivered != -1 and cur_salary_id != -1,abs(cur_salary_id-jd_user_cur_salary_id_avg_delivered),-1) jd_user_cur_salary_id_avg_delivered_deltabyactual
        ,if(jd_user_cur_degree_id_avg_delivered != -1 and cur_degree_id != -1,abs(cur_degree_id-jd_user_cur_degree_id_avg_delivered),-1) jd_user_cur_degree_id_avg_delivered_deltabyactual
        ,if(jd_user_age_avg_delivered != -1 and age != -1,abs(age-jd_user_age_avg_delivered),-1) jd_user_age_avg_delivered_deltabyactual
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
        ,if(jd_user_user_work_years_avg_satisfied != -1 and user_work_years != -1,abs(user_work_years-jd_user_user_work_years_avg_satisfied),-1) jd_user_user_work_years_avg_satisfied_deltabyactual
        ,if(jd_user_desire_jd_salary_id_avg_satisfied != -1 and desire_jd_salary_id != -1,abs(desire_jd_salary_id-jd_user_desire_jd_salary_id_avg_satisfied),-1) jd_user_desire_jd_salary_id_avg_satisfied_deltabyactual
        ,if(jd_user_cur_salary_id_avg_satisfied != -1 and cur_salary_id != -1,abs(cur_salary_id-jd_user_cur_salary_id_avg_satisfied),-1) jd_user_cur_salary_id_avg_satisfied_deltabyactual
        ,if(jd_user_cur_degree_id_avg_satisfied != -1 and cur_degree_id != -1,abs(cur_degree_id-jd_user_cur_degree_id_avg_satisfied),-1) jd_user_cur_degree_id_avg_satisfied_deltabyactual
        ,if(jd_user_age_avg_satisfied != -1 and age != -1,abs(age-jd_user_age_avg_satisfied),-1) jd_user_age_avg_satisfied_deltabyactual
from    (
            select  t1.user_id
                    ,t1.jd_no
                    ---
                    ,coalesce(sum(if(browsed != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_count_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_ratio_browsed
                    ,coalesce(sum(if(browsed != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_count_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_ratio_browsed
                    ,coalesce(sum(if(browsed != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_count_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_ratio_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.user_work_years = -1,null,t3.user_work_years)),-1) as jd_user_user_work_years_avg_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.desire_jd_salary_id = -1,null,t3.desire_jd_salary_id)),-1) as jd_user_desire_jd_salary_id_avg_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.cur_salary_id = -1,null,t3.cur_salary_id)),-1) as jd_user_cur_salary_id_avg_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.cur_degree_id = -1,null,t3.cur_degree_id)),-1) as jd_user_cur_degree_id_avg_browsed
                    ,coalesce(avg(if(browsed != 1 or t3.age = -1,null,t3.age)),-1) as jd_user_age_avg_browsed
                    ----------------------------
                    ,coalesce(sum(if(delivered != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_count_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_ratio_delivered
                    ,coalesce(sum(if(delivered != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_count_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_ratio_delivered
                    ,coalesce(sum(if(delivered != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_count_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_ratio_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.user_work_years = -1,null,t3.user_work_years)),-1) as jd_user_user_work_years_avg_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.desire_jd_salary_id = -1,null,t3.desire_jd_salary_id)),-1) as jd_user_desire_jd_salary_id_avg_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.cur_salary_id = -1,null,t3.cur_salary_id)),-1) as jd_user_cur_salary_id_avg_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.cur_degree_id = -1,null,t3.cur_degree_id)),-1) as jd_user_cur_degree_id_avg_delivered
                    ,coalesce(avg(if(delivered != 1 or t3.age = -1,null,t3.age)),-1) as jd_user_age_avg_delivered
                    ----------------------------
                    ,coalesce(sum(if(satisfied != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_count_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.cur_city_id = -1 or t4.cur_city_id = -1,null,1)* if(t3.cur_city_id=t4.cur_city_id,1,0)),-1) as jd_user_cur_city_id_ratio_satisfied
                    ,coalesce(sum(if(satisfied != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_count_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.cur_industry_id = -1 or t4.cur_industry_id = -1,null,1)* if(t3.cur_industry_id=t4.cur_industry_id,1,0)),-1) as jd_user_cur_industry_id_ratio_satisfied
                    ,coalesce(sum(if(satisfied != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_count_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.cur_jd_type_id = -1 or t4.cur_jd_type_id = -1,null,1)* if(t3.cur_jd_type_id=t4.cur_jd_type_id,1,0)),-1) as jd_user_cur_jd_type_id_ratio_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.user_work_years = -1,null,t3.user_work_years)),-1) as jd_user_user_work_years_avg_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.desire_jd_salary_id = -1,null,t3.desire_jd_salary_id)),-1) as jd_user_desire_jd_salary_id_avg_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.cur_salary_id = -1,null,t3.cur_salary_id)),-1) as jd_user_cur_salary_id_avg_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.cur_degree_id = -1,null,t3.cur_degree_id)),-1) as jd_user_cur_degree_id_avg_satisfied
                    ,coalesce(avg(if(satisfied != 1 or t3.age = -1,null,t3.age)),-1) as jd_user_age_avg_satisfied
            from    (
                        select  distinct user_id
                                ,jd_no
                                ,kfold_flag
                        from    zhaopin_round2_all_action
                    ) t1
            left outer join zhaopin_round2_all_action2 t2
            on      t1.jd_no = t2.jd_no
            and     t1.kfold_flag = t2.kfold_flag
            and     t2.browsed = 1
            left outer join zhaopin_round2_user_p t3
            on      t2.user_id = t3.user_id
            join    zhaopin_round2_user_p t4
            on      t1.user_id = t4.user_id
            group by t1.user_id
                     ,t1.jd_no
        ) p1
join    zhaopin_round2_user_p p2
on      p1.user_id = p2.user_id
;


--i2i 看相似jd的点击 投递 满意率情况
drop table if exists zhaopin_round2_represent_similar_jd;
create table if not exists zhaopin_round2_represent_similar_jd as
select  *
        --统计特征，暂略
from    (
            select  p1.user_id
                    ,p1.jd_no
                    ,coalesce(jd_similarity_ratio_11,0) jd_similarity_ratio_11
                    ,coalesce(jd_similarity_ratio_12,0) jd_similarity_ratio_12
                    ,coalesce(jd_similarity_ratio_13,0) jd_similarity_ratio_13
                    ,coalesce(jd_similarity_ratio_21,0) jd_similarity_ratio_21
                    ,coalesce(jd_similarity_ratio_22,0) jd_similarity_ratio_22
                    ,coalesce(jd_similarity_ratio_23,0) jd_similarity_ratio_23
                    ,coalesce(jd_similarity_ratio_31,0) jd_similarity_ratio_31
                    ,coalesce(jd_similarity_ratio_32,0) jd_similarity_ratio_32
                    ,coalesce(jd_similarity_ratio_33,0) jd_similarity_ratio_33
            from    (
                        select distinct user_id
                                ,jd_no
                                ,kfold_flag
                        from    zhaopin_round2_all_action
                        --where   jd_no in ('00000d0857555ef70b740cfdf0109a19','66e2fdfbe5d225a55e3bfc4af91c2111')
                    ) p1
            left outer join (
                                select  t3.kfold_flag
                                        ,original_id
                                        ,if(sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt))>0,coalesce(sum(browsed_cnt*total_browsed_cnt/(if(total_browsed_cnt=0,null,total_browsed_cnt)*total_cnt)
                                        )/sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt)),0),0) as jd_similarity_ratio_11
                                        ,if(sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt))>0,coalesce(sum(browsed_cnt*total_delivered_cnt/(if(total_browsed_cnt=0,null,total_browsed_cnt)*total_cnt)
                                        )/sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt)),0),0) as jd_similarity_ratio_12
                                        ,if(sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt))>0,coalesce(sum(browsed_cnt*total_satisfied_cnt/(if(total_browsed_cnt=0,null,total_browsed_cnt)*total_cnt)
                                        )/sum(browsed_cnt/if(total_browsed_cnt=0,null,total_browsed_cnt)),0),0) as jd_similarity_ratio_13
                                        --
                                        ,if(sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt))>0,coalesce(sum(delivered_cnt*total_browsed_cnt/(if(total_delivered_cnt=0,null,total_delivered_cnt)*total_cnt)
                                        )/sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt)),0),0) as jd_similarity_ratio_21
                                        ,if(sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt))>0,coalesce(sum(delivered_cnt*total_delivered_cnt/(if(total_delivered_cnt=0,null,total_delivered_cnt)*total_cnt)
                                        )/sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt)),0),0) as jd_similarity_ratio_22
                                        ,if(sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt))>0,coalesce(sum(delivered_cnt*total_satisfied_cnt/(if(total_delivered_cnt=0,null,total_delivered_cnt)*total_cnt)
                                        )/sum(delivered_cnt/if(total_delivered_cnt=0,null,total_delivered_cnt)),0),0) as jd_similarity_ratio_23
                                        --
                                        ,if(sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt))>0,coalesce(sum(satisfied_cnt*total_browsed_cnt/(if(total_satisfied_cnt=0,null,total_satisfied_cnt)*total_cnt)
                                        )/sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt)),0),0) as jd_similarity_ratio_31
                                        ,if(sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt))>0,coalesce(sum(satisfied_cnt*total_delivered_cnt/(if(total_satisfied_cnt=0,null,total_satisfied_cnt)*total_cnt)
                                        )/sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt)),0),0) as jd_similarity_ratio_32
                                        ,if(sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt))>0,coalesce(sum(satisfied_cnt*total_satisfied_cnt/(if(total_satisfied_cnt=0,null,total_satisfied_cnt)*total_cnt)
                                        )/sum(satisfied_cnt/if(total_satisfied_cnt=0,null,total_satisfied_cnt)),0),0) as jd_similarity_ratio_33
                                from    (
                                            select  t1.kfold_flag
                                                    ,t1.jd_no as original_id
                                                    ,t2.jd_no as near_id
                                                    ,count(*) as browsed_cnt
                                                    --,count(if(t1.browsed=1 and t2.browsed = 1,1,null)) as browsed_cnt
                                                    ,count(if(t1.delivered=1 and t2.delivered = 1,1,null)) as delivered_cnt
                                                    ,count(if(t1.satisfied=1 and t2.satisfied = 1,1,null)) as satisfied_cnt
                                            from    zhaopin_round2_all_action2 t1
                                            join    zhaopin_round2_all_action2 t2
                                            on t1.kfold_flag = t2.kfold_flag and t1.user_id  = t2.user_id
                                            and  t1.browsed = 1 and t2.browsed = 1
                                            group by t1.kfold_flag,t1.jd_no,t2.jd_no
                                        ) t3
                                        left outer join (
                                                    select  kfold_flag
                                                            ,jd_no
                                                            ,count(*) as total_cnt
                                                            ,int(sum(browsed)) as total_browsed_cnt
                                                            ,int(sum(delivered)) as total_delivered_cnt
                                                            ,int(sum(satisfied)) as total_satisfied_cnt
                                                    from     zhaopin_round2_all_action2
                                                    group by kfold_flag,jd_no
                                                ) t4
                                on      t3.near_id = t4.jd_no
                                and     t3.kfold_flag = t4.kfold_flag
                                group by t3.kfold_flag,original_id
                            ) p2
            on      p1.jd_no = p2.original_id and p1.kfold_flag = p2.kfold_flag
        ) p3
;
-----------------------------过滤掉只出现一次的
--experience set
drop table if exists zhaopin_round2_exp_word_set ;

create table if not exists zhaopin_round2_exp_word_set as
select  experience
from    (
            select  experience
                    ,count(distinct  user_id) as cnt
            from    (
                        select  trans_array(
                                    1
                                    ,'|'
                                    ,user_id
                                    ,replace(experience,'null','')
                                ) as (user_id,experience)
                        from    zhaopin_round2_user
                    ) t1
            where   length(experience) > 1
            group by experience
        ) t2
where   cnt > 1
;
--job_des_word_set
drop table if exists zhaopin_round2_job_des_word_set ;

create table if not exists zhaopin_round2_job_des_word_set as
select  job_description
from    (
            select  job_description
                    ,count(distinct jd_no) as cnt
            from    (
                        -- zhaopin_round2_keyword & zhaopin_round2_splitword 都是在pai上生成的 （关键词取top20个）
                        select  jd_no
                                ,keywords as job_description
                        from    zhaopin_round2_keyword
                        union
                        select  *
                        from    (
                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                    from    zhaopin_round2_splitword
                                ) p1
                        where   length(job_description) > 1
                    ) p2
            group by job_description
        ) t3
where   cnt > 1
;

--experience 与 job_description 做交叉
drop table if exists zhaopin_round2_exp_des_similarity ;

create table if not exists zhaopin_round2_exp_des_similarity as
select  *
        --统计特征，暂略
from    (
            select  user_id
                    ,jd_no
                    ,coalesce(avg(browsed_r),0) as exp_des_browsed_r
                    ,coalesce(max(browsed_r),0) as exp_des_browsed_r_max
                    ,coalesce(stddev(browsed_r),0) as exp_des_browsed_r_std
                    ,coalesce(avg(delivered_r),0) as exp_des_delivered_r
                    ,coalesce(max(delivered_r),0) as exp_des_delivered_r_max
                    ,coalesce(stddev(delivered_r),0) as exp_des_delivered_r_std
                    ,coalesce(avg(satisfied_r),0) as exp_des_satisfied_r
                    ,coalesce(max(satisfied_r),0) as exp_des_satisfied_r_max
                    ,coalesce(stddev(satisfied_r),0) as exp_des_satisfied_r_std
            from    (
                        select  t1.kfold_flag
                                ,t1.user_id
                                ,t1.jd_no
                                ,experience
                                ,job_description
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                            ,kfold_flag
                                    from    zhaopin_round2_all_action
                                --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                ) t1
                        left outer join (
                                            select  user_id
                                                    ,p1.experience
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,'|'
                                                                    ,user_id
                                                                    ,replace(experience,'null','')
                                                                ) as (user_id,experience)
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            join    zhaopin_round2_exp_word_set p2
                                            on      p1.experience = p2.experience
                                        ) t2
                        on      t1.user_id = t2.user_id
                        left outer join (
                                            -- zhaopin_round2_keyword & zhaopin_round2_splitword 都是在pai上生成的
                                            select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  jd_no
                                                                ,keywords as job_description
                                                        from    zhaopin_round2_keyword
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            join    zhaopin_round2_job_des_word_set p3
                                            on      p2.job_description = p3.job_description
                                        ) t3
                        on      t1.jd_no = t3.jd_no
                    ) p1
            left outer join (
                                select  kfold_flag
                                        ,t2.experience
                                        ,t3.job_description
                                        ,avg(browsed) as browsed_r
                                        ,avg(delivered) as delivered_r
                                        ,avg(satisfied) as satisfied_r
                                from    (
                                            select  distinct user_id
                                                    ,jd_no
                                                    ,browsed
                                                    ,delivered
                                                    ,satisfied
                                                    ,kfold_flag
                                            from    zhaopin_round2_all_action2
                                            --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                        ) t1
                                join    (
                                            select  user_id
                                                    ,p1.experience
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,'|'
                                                                    ,user_id
                                                                    ,replace(experience,'null','')
                                                                ) as (user_id,experience)
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            join    zhaopin_round2_exp_word_set p2
                                            on      p1.experience = p2.experience
                                        ) t2
                                on      t1.user_id = t2.user_id
                                join    (
                                            -- zhaopin_round2_keyword & zhaopin_round2_splitword 都是在pai上生成的
                                            select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  jd_no
                                                                ,keywords as job_description
                                                        from    zhaopin_round2_keyword
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            join    zhaopin_round2_job_des_word_set p3
                                            on      p2.job_description = p3.job_description
                                        ) t3
                                on      t1.jd_no = t3.jd_no
                                group by kfold_flag
                                         ,t2.experience
                                         ,t3.job_description
                            ) p2
            on      p1.kfold_flag = p2.kfold_flag
            and     p1.experience = p2.experience
            and     p1.job_description = p2.job_description
            group by user_id
                     ,jd_no
        ) p3
;

--experience 在 job_description 能够找到的个数
drop table if exists zhaopin_round2_exp_des_similarity2 ;

create table if not exists zhaopin_round2_exp_des_similarity2 as
select  *
        ,avg(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_avgbyuser
        ,max(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_maxbyuser
        ,min(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_minbyuser
        ,stddev(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_stdbyuser
        ,rank() over(partition by user_id order by coalesce(exp_find_ratio,0) desc) as exp_find_ratio_rankbyuser
        ,percent_rank() over(partition by user_id order by coalesce(exp_find_ratio,0) desc) as exp_find_ratio_rankbyuser_percentage
        ,coalesce(exp_find_ratio,0) - avg(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_deltabyuseravg
        ,coalesce(exp_find_ratio,0) - max(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_deltabyusermax
        ,coalesce(exp_find_ratio,0) - min(coalesce(exp_find_ratio,0)) over(partition by user_id ) as exp_find_ratio_deltabyusermin
from    (
            select  p1.user_id
                    ,jd_no
                    ,count(*) as exp_cnt
                    ,sum(is_find) as exp_find_cnt
                    ,avg(is_find) as exp_find_ratio
            from    (
                        select  t1.user_id
                                ,t1.jd_no
                                ,experience
                                ,job_description
                                ,if(instr(job_description,experience)>0,1,0 ) as is_find
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                    from    zhaopin_round2_all_action
                                --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                ) t1
                        left outer join (
                                            select  user_id
                                                    ,p1.experience
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,'|'
                                                                    ,user_id
                                                                    ,replace(experience,'null','')
                                                                ) as (user_id,experience)
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            join    zhaopin_round2_exp_word_set p2
                                            on      p1.experience = p2.experience
                                        ) t2
                        on      t1.user_id = t2.user_id
                        left outer join (
                                            select jd_no
                                                    ,concat(jd_title,key,job_description) as job_description
                                            from zhaopin_round2_jd
                                        ) t3
                        on      t1.jd_no = t3.jd_no
                    ) p1
            group by p1.user_id
                     ,jd_no
        ) p3
;


--industry &  job_description
drop table if exists zhaopin_round2_industry_des_similarity ;

create table if not exists zhaopin_round2_industry_des_similarity as
select  *
        --统计特征，暂略
from    (
            select  user_id
                    ,jd_no
                    ,avg(coalesce(browsed_r,0)) as industry_des_browsed_r
                    ,max(coalesce(browsed_r,0))as industry_des_browsed_r_max
                    ,stddev(coalesce(browsed_r,0)) as industry_des_browsed_r_std
                    ,avg(coalesce(delivered_r,0)) as industry_des_delivered_r
                    ,max(coalesce(delivered_r,0)) as industry_des_delivered_r_max
                    ,stddev(coalesce(delivered_r,0)) as industry_des_delivered_r_std
                    ,avg(coalesce(satisfied_r,0)) as industry_des_satisfied_r
                    ,max(coalesce(satisfied_r,0)) as industry_des_satisfied_r_max
                    ,stddev(coalesce(satisfied_r,0)) as industry_des_satisfied_r_std
            from    (
                        select  t1.kfold_flag
                                ,t1.user_id
                                ,t1.jd_no
                                ,industry_id
                                ,job_description
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                            ,kfold_flag
                                    from    zhaopin_round2_all_action
                                --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                ) t1
                        left outer join (
                                            select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_industry_id
                                                                ) as (user_id,industry_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_industry_id as industry_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   industry_id != ''
                                        ) t2
                        on      t1.user_id = t2.user_id
                        left outer join (
                                            select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,job_description) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            where  length(job_description)>1
                                        ) t3
                        on      t1.jd_no = t3.jd_no
                    ) p1
            left outer join (
                                select  kfold_flag
                                        ,t2.industry_id
                                        ,t3.job_description
                                        ,avg(browsed) as browsed_r
                                        ,avg(delivered) as delivered_r
                                        ,avg(satisfied) as satisfied_r
                                from    (
                                            select  distinct user_id
                                                    ,jd_no
                                                    ,browsed
                                                    ,delivered
                                                    ,satisfied
                                                    ,kfold_flag
                                            from    zhaopin_round2_all_action2
                                        --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                        ) t1
                                join    (
                                            select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_industry_id
                                                                ) as (user_id,industry_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_industry_id as industry_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   industry_id != ''
                                        ) t2
                                on      t1.user_id = t2.user_id
                                join    (
                                            select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,job_description) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            where  length(job_description)>1
                                        ) t3
                                on      t1.jd_no = t3.jd_no
                                group by kfold_flag
                                         ,t2.industry_id
                                         ,t3.job_description
                            ) p2
            on      p1.kfold_flag = p2.kfold_flag
            and     p1.industry_id = p2.industry_id
            and     p1.job_description = p2.job_description
            group by user_id
                     ,jd_no
        ) p3
;

--jd_type &  job_description
drop table if exists zhaopin_round2_type_des_similarity ;

create table if not exists zhaopin_round2_type_des_similarity as
select  *
        --统计特征，暂略
from    (
            select  user_id
                    ,jd_no
                    ,avg(coalesce(browsed_r,0)) as type_des_browsed_r
                    ,max(coalesce(browsed_r,0))as type_des_browsed_r_max
                    ,stddev(coalesce(browsed_r,0)) as type_des_browsed_r_std
                    ,avg(coalesce(delivered_r,0)) as type_des_delivered_r
                    ,max(coalesce(delivered_r,0)) as type_des_delivered_r_max
                    ,stddev(coalesce(delivered_r,0)) as type_des_delivered_r_std
                    ,avg(coalesce(satisfied_r,0)) as type_des_satisfied_r
                    ,max(coalesce(satisfied_r,0)) as type_des_satisfied_r_max
                    ,stddev(coalesce(satisfied_r,0)) as type_des_satisfied_r_std
            from    (
                        select  t1.kfold_flag
                                ,t1.user_id
                                ,t1.jd_no
                                ,jd_type_id
                                ,job_description
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                            ,kfold_flag
                                    from    zhaopin_round2_all_action
                                --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                ) t1
                        left outer join (
                                            select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_type_id
                                                                ) as (user_id,jd_type_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_jd_type as jd_type_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   jd_type_id != ''
                                        ) t2
                        on      t1.user_id = t2.user_id
                        left outer join (
                                           select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,job_description) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            where  length(job_description)>1
                                        ) t3
                        on      t1.jd_no = t3.jd_no
                    ) p1
            left outer join (
                                select  kfold_flag
                                        ,t2.jd_type_id
                                        ,t3.job_description
                                        ,avg(browsed) as browsed_r
                                        ,avg(delivered) as delivered_r
                                        ,avg(satisfied) as satisfied_r
                                from    (
                                            select  distinct user_id
                                                    ,jd_no
                                                    ,browsed
                                                    ,delivered
                                                    ,satisfied
                                                    ,kfold_flag
                                            from    zhaopin_round2_all_action2
                                        --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                        ) t1
                                join    (
                                             select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_type_id
                                                                ) as (user_id,jd_type_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_jd_type as jd_type_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   jd_type_id != ''
                                        ) t2
                                on      t1.user_id = t2.user_id
                                join    (
                                            -- zhaopin_round2_keyword & zhaopin_round2_splitword 都是在pai上生成的
                                            select  jd_no
                                                    ,p2.job_description
                                            from    (
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,job_description) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                        union
                                                        select  *
                                                        from    (
                                                                    select  trans_array(1,' ',jd_no,jd_title) as (jd_no,job_description)
                                                                    from    zhaopin_round2_splitword
                                                                ) p1
                                                    ) p2
                                            where  length(job_description)>1
                                        ) t3
                                on      t1.jd_no = t3.jd_no
                                group by kfold_flag
                                         ,t2.jd_type_id
                                         ,t3.job_description
                            ) p2
            on      p1.kfold_flag = p2.kfold_flag
            and     p1.jd_type_id = p2.jd_type_id
            and     p1.job_description = p2.job_description
            group by user_id
                     ,jd_no
        ) p3
;


--industry & jd_type_id
drop table if exists zhaopin_round2_industry_type_similarity ;

create table if not exists zhaopin_round2_industry_type_similarity as
select  *
        --统计特征，暂略
from    (
            select  user_id
                    ,jd_no
                    ,avg(coalesce(browsed_r,0)) as industry_type_browsed_r
                    ,max(coalesce(browsed_r,0))as industry_type_browsed_r_max
                    ,stddev(coalesce(browsed_r,0)) as industry_type_browsed_r_std
                    ,avg(coalesce(delivered_r,0)) as industry_type_delivered_r
                    ,max(coalesce(delivered_r,0)) as industry_type_delivered_r_max
                    ,stddev(coalesce(delivered_r,0)) as industry_type_delivered_r_std
                    ,avg(coalesce(satisfied_r,0)) as industry_type_satisfied_r
                    ,max(coalesce(satisfied_r,0)) as industry_type_satisfied_r_max
                    ,stddev(coalesce(satisfied_r,0)) as industry_type_satisfied_r_std
            from    (
                        select  t1.kfold_flag
                                ,t1.user_id
                                ,t1.jd_no
                                ,industry_id
                                ,jd_type_id
                        from    (
                                    select  distinct user_id
                                            ,jd_no
                                            ,kfold_flag
                                    from    zhaopin_round2_all_action
                                --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                ) t1
                        left outer join (
                                            select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_industry_id
                                                                ) as (user_id,industry_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_industry_id as industry_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   industry_id != ''
                                        ) t2
                        on      t1.user_id = t2.user_id
                        left outer join zhaopin_round2_jd_p t3
                        on      t1.jd_no = t3.jd_no
                    ) p1
            left outer join (
                                select  kfold_flag
                                        ,t2.industry_id
                                        ,t3.jd_type_id
                                        ,avg(browsed) as browsed_r
                                        ,avg(delivered) as delivered_r
                                        ,avg(satisfied) as satisfied_r
                                from    (
                                            select  distinct user_id
                                                    ,jd_no
                                                    ,browsed
                                                    ,delivered
                                                    ,satisfied
                                                    ,kfold_flag
                                            from    zhaopin_round2_all_action2
                                        --where   user_id = 'edcad0ca3dca86591994354bd95257f9'
                                        ) t1
                                join    (
                                            select  *
                                            from    (
                                                        select  trans_array(
                                                                    1
                                                                    ,','
                                                                    ,user_id
                                                                    ,desire_jd_industry_id
                                                                ) as (user_id,industry_id)
                                                        from    zhaopin_round2_user
                                                        union
                                                        select  user_id
                                                                ,cur_industry_id as industry_id
                                                        from    zhaopin_round2_user
                                                    ) p1
                                            where   industry_id != ''
                                        ) t2
                                on      t1.user_id = t2.user_id
                                join    zhaopin_round2_jd_p t3
                                on      t1.jd_no = t3.jd_no
                                group by kfold_flag
                                         ,t2.industry_id
                                         ,t3.jd_type_id
                            ) p2
            on      p1.kfold_flag = p2.kfold_flag
            and     p1.jd_type_id = p2.jd_type_id
            and     p1.industry_id = p2.industry_id
            group by user_id
                     ,jd_no
        ) p3
;