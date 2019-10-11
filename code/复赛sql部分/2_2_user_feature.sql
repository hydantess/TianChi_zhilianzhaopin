--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-27 17:23:12
--********************************************************************--
drop table if exists zhaopin_round2_user_p ;

create table if not exists zhaopin_round2_user_p as
select  p1.user_id
        ,kfold_flag
        --city
        ,coalesce(city_id,-1) as cur_city_id
        ,coalesce(desire_jd_city_id,'') as desire_jd_city_id
        --industry
        ,coalesce(industry_id,-1) as cur_industry_id
        ,coalesce(desire_jd_industry_id,'') as desire_jd_industry_id
        --jd_type
        ,coalesce(jd_type_id,-1) as cur_jd_type_id
        ,coalesce(desire_jd_type_id,'') as desire_jd_type_id
        --其他
        ,if(
            birthday in ('-','0','3')
            ,-1
            ,cast(birthday as bigint )
        )as age
        --不知道用户实际求职时间，这里统一使用‘201908’，误差影响应该不大
        ,case    when start_work_date = '-' or start_work_date like '%00' or start_work_date is null then -1
                 when datediff(to_date('201908','yyyymm'), to_date(start_work_date,'yyyymm'),'mm')/12 <0.5 then 0
                when datediff(to_date('201908','yyyymm'), to_date(start_work_date,'yyyymm'),'mm')/12 <3 then 1
                when datediff(to_date('201908','yyyymm'), to_date(start_work_date,'yyyymm'),'mm')/12 <5 then 2
                when datediff(to_date('201908','yyyymm'), to_date(start_work_date,'yyyymm'),'mm')/12 <10 then 3
                else 4
        end as user_work_years
        ,case    when desire_jd_salary_id in ('0000000000') then -1
                 when desire_jd_salary_id in ('0000001000') then 0
                 when desire_jd_salary_id in ('0100002000') then 1
                 when desire_jd_salary_id in ('0200104000') then 2
                 when desire_jd_salary_id in ('0400106000') then 3
                 when desire_jd_salary_id in ('0600108000') then 4
                 when desire_jd_salary_id in ('0800110000') then 5
                 when desire_jd_salary_id in ('1000115000') then 6
                 when desire_jd_salary_id in ('1500125000') then 7
                 else 8
         end as desire_jd_salary_id
        ,case    when cur_salary_id in ('0000000000','-') then -1
                 when cur_salary_id in ('0000001000') then 0
                 when cur_salary_id in ('0100002000') then 1
                 when cur_salary_id in ('0200104000') then 2
                 when cur_salary_id in ('0400106000') then 3
                 when cur_salary_id in ('0600108000') then 4
                 when cur_salary_id in ('0800110000') then 5
                 when cur_salary_id in ('1000115000') then 6
                 when cur_salary_id in ('1500125000') then 7
                 else 8
         end as cur_salary_id
        ,case    when trim(cur_degree_id) in ('硕士', '博士','MBA', 'EMBA') then 4
                 when trim(cur_degree_id) in ('本科') then 3
                 when trim(cur_degree_id) in ('大专') then 2
                 when trim(cur_degree_id) in ('中专','中技','高中','初中') then 1
                 else -1
         end as cur_degree_id
from    zhaopin_round2_user p1
left outer join (
                    select  user_id
                            ,row_number() over() % 10 as kfold_flag
                    from    (
                                select  distinct user_id
                                from    zhaopin_round2_action_train
                            ) t1
                    union
                    --两表user_id无重合
                    select  distinct user_id
                            ,-1 as kfold_flag
                    from    zhaopin_round2_action_test
                ) p2
on      p1.user_id = p2.user_id
left outer join zhaopin_round2_city p2
on      p1.live_city_id = p2.city
left outer join zhaopin_round2_jd_type p3
on      p1.cur_jd_type = p3.jd_type
left outer join zhaopin_round2_industry p4
on      p1.cur_industry_id = p4.industry
;
