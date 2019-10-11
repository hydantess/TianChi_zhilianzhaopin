--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-09-09 08:55:46
--********************************************************************--
drop table if exists zhaopin_round2_final_sub ;

create table if not exists zhaopin_round2_final_sub as
select  user_id
        ,jd_no
        ,avg(final_score) as final_score
        ,avg(final_rank) as final_rank
from    (
            select  user_id
                    ,jd_no
                    ,0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                    ,rank() over(partition by user_id order by 0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) desc) as final_rank
            from    zhaopin_round2_final_pre0
            union all
            select  user_id
                    ,jd_no
                    ,0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                    ,rank() over(partition by user_id order by 0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) desc) as final_rank
            from    zhaopin_round2_final_pre1
            union all
            select  user_id
                    ,jd_no
                    ,0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                    ,rank() over(partition by user_id order by 0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) desc) as final_rank
            from    zhaopin_round2_final_pre2
            union all
            select  user_id
                    ,jd_no
                    ,0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                    ,rank() over(partition by user_id order by 0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) desc) as final_rank
            from    zhaopin_round2_final_pre3
            union all
            select  user_id
                    ,jd_no
                    ,0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                    ,rank() over(partition by user_id order by 0.2*coalesce(get_json_object(prediction_detail,'$.1'),0)+coalesce(get_json_object(prediction_detail,'$.2'),0) desc) as final_rank
            from    zhaopin_round2_final_pre4
        ) p1
group by user_id
         ,jd_no
;

--submit

drop table if exists zhaopin_round2_submit ;

create table if not exists zhaopin_round2_submit as
select  user_id
        ,jd_no
        -- ,final_score
        -- ,final_rank
        ,row_number() over(partition by flag order by user_id,final_score desc) as index_evaluate
from    (
            select  *
                    ,1 as flag
            from    zhaopin_round2_final_sub
        ) t1
;

--check count

select  count(*)
from    zhaopin_round2_submit
;

--计算3classes map 系数0.2根据验证集确定

select  kfold_flag
        ,0.3*avg(map_delivered)+0.7*avg(map_satisfied) as map_final
from    (
            select  kfold_flag
                    ,user_id
                    ,avg(map_delivered_each) as map_delivered
                    ,avg(map_satisfied_each) as map_satisfied
            from    (
                        select  kfold_flag
                                ,user_id
                                ,delivered_flag *row_number() over(partition by user_id, delivered_flag order by rank_1 )/ rank_1 as map_delivered_each
                                ,satisfied_flag *row_number() over(partition by user_id, satisfied_flag order by rank_1 )/ rank_1 as map_satisfied_each
                        from    (
                                    select  *
                                            ,row_number() over(partition by user_id order by final_score desc) as rank_1
                                            ,if(label_3classes>=1,1,null) as delivered_flag
                                            ,if(label_3classes=2,1,null) as satisfied_flag
                                    from    (
                                                select  user_id
                                                        ,jd_no
                                                        ,kfold_flag
                                                        ,label_2classes_satisfied
                                                        ,label_3classes
                                                        --3classes
                                                        ,0.2*coalesce(
                                                            get_json_object(prediction_detail,'$.1')
                                                            ,0
                                                        )+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                                                from    zhaopin_round2_final_val0
                                                union all
                                                select  user_id
                                                        ,jd_no
                                                        ,kfold_flag
                                                        ,label_2classes_satisfied
                                                        ,label_3classes
                                                        --3classes
                                                        ,0.2*coalesce(
                                                            get_json_object(prediction_detail,'$.1')
                                                            ,0
                                                        )+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                                                from    zhaopin_round2_final_val1
                                                union all
                                                select  user_id
                                                        ,jd_no
                                                        ,kfold_flag
                                                        ,label_2classes_satisfied
                                                        ,label_3classes
                                                        --3classes
                                                        ,0.2*coalesce(
                                                            get_json_object(prediction_detail,'$.1')
                                                            ,0
                                                        )+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                                                from    zhaopin_round2_final_val2
                                                union all
                                                select  user_id
                                                        ,jd_no
                                                        ,kfold_flag
                                                        ,label_2classes_satisfied
                                                        ,label_3classes
                                                        --3classes
                                                        ,0.2*coalesce(
                                                            get_json_object(prediction_detail,'$.1')
                                                            ,0
                                                        )+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                                                from    zhaopin_round2_final_val3
                                                union all
                                                select  user_id
                                                        ,jd_no
                                                        ,kfold_flag
                                                        ,label_2classes_satisfied
                                                        ,label_3classes
                                                        --3classes
                                                        ,0.2*coalesce(
                                                            get_json_object(prediction_detail,'$.1')
                                                            ,0
                                                        )+coalesce(get_json_object(prediction_detail,'$.2'),0) as final_score
                                                from    zhaopin_round2_final_val4
                                            ) t1
                                ) t2
                    ) t3
            group by kfold_flag
                     ,user_id
        ) t3
group by kfold_flag
;
