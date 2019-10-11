--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-27 17:23:41
--********************************************************************--
---------------------------------------手动转int
--city
drop table if exists zhaopin_round2_city ;

create table if not exists zhaopin_round2_city as
select  city
        ,row_number() over() as city_id
from    (
            select  distinct city
            from    (
                        select  trans_array(0,',',desire_jd_city_id) as (city)
                        from    zhaopin_round2_user
                        union all
                        select  live_city_id as city
                        from    zhaopin_round2_user
                        union all
                        select  city
                        from    zhaopin_round2_jd
                    ) t1
            where   city is not null
            and     city not in ('','-')
        ) t2
;

--industry
drop table if exists zhaopin_round2_industry ;

create table if not exists zhaopin_round2_industry as
select  industry
        ,row_number() over() as industry_id
from    (
            select  distinct industry
            from    (
                        select  trans_array(0,',',desire_jd_industry_id) as (industry)
                        from    zhaopin_round2_user
                        union all
                        select  cur_industry_id as industry
                        from    zhaopin_round2_user
                    ) t1
            where   industry is not null
            and     industry != ''
        ) t2
;


--jd_type
drop table if exists zhaopin_round2_jd_type ;

create table if not exists zhaopin_round2_jd_type as
select  jd_type
        ,row_number() over() as jd_type_id
from    (
            select  distinct jd_type
            from    (
                        select  trans_array(0,',',desire_jd_type_id) as (jd_type)
                        from    zhaopin_round2_user
                        union all
                        select  cur_jd_type as jd_type
                        from    zhaopin_round2_user
                        union all
                        select  jd_sub_type as jd_type
                        from    zhaopin_round2_jd
                    ) t1
            where   jd_type is not null
            and     jd_type != ''
        ) t2
;


---------------------------------------正则提取jd表信息
drop table if exists zhaopin_round2_jd_r ;

create table if not exists zhaopin_round2_jd_r as
select  p1.jd_no
        ,min_edu_level_2
        ,if(jd_age_min<18 or jd_age_min>=65,-1,int(jd_age_min/3)*3) as age_1
        ,if(jd_age_max<18 or jd_age_max>=65,-1,int(jd_age_max/3)*3) as age_2
        ,min_years_2
        --提取的薪资信息噪音比较大,原字段缺失比例少，故弃用
        -- ,if(min_salary_2=1000000,-1,min_salary_2) as min_salary_2
        -- ,if(max_salary_2=min_salary_2,-1,max_salary_2) as max_salary_2
from    (    --抽取学历信息
            select  jd_no
                    ,case    when reg1 like '%中专%' or reg1 like'%中技%' or reg1 like'%高中%' or reg1 like'%初中%' or reg1 like'%不限%' then 1
                             when reg1 like '%专科%' or reg1 like'%大专%' then 2
                             when reg1 like '%本科%' or reg1 like'%一本%' or reg1 like'%二本%' or reg1 like'%三本%' or reg1 like '%大学%' then 3
                             when reg1 like '%硕士%' or reg1 like'%博士%' or reg1 like '%研究生%' then 4
                             else -1
                     end as min_edu_level_2
            from    (
                        select  *
                                ,regexp_extract(
                                    job_description
                                    ,'[^0-9]{0,10}(学历|文凭|学位|文化程度|文化水平)[^0-9]{0,10}'
                                    ,0
                                ) as reg1
                        from    zhaopin_round2_jd
                    ) t1
        ) p1
join    (    --抽取年龄信息
            select  jd_no
                    ,case    when reg_max_ != '' then cast(reg_max_ as bigint )
                             when reg_max != '' then cast(reg_max as bigint )
                             when reg_mid !='' then cast(reg_mid+5 as bigint )
                             when reg_left!='' then  cast(greatest(reg_left,reg_right) as bigint )
                             else -1
                     end as jd_age_max
                    ,case    when reg_min_ != '' then cast(reg_min_ as bigint )
                             when reg_min !='' then cast(reg_min as bigint )
                             when reg_mid !='' then cast(reg_mid-5 as bigint )
                             when reg_left!='' then cast(least(reg_left,reg_right)  as bigint )
                             else -1
                     end as jd_age_min
            from    (
                        select  *
                                ,regexp_extract(
                                    reg1
                                    ,'[^0-9]+([1-9][0-9])[^0-9]+?([1-9][0-9])[^0-9]+'
                                    ,1
                                ) as reg_left
                                ,regexp_extract(
                                    reg1
                                    ,'[^0-9]+([1-9][0-9])[^0-9]+?([1-9][0-9])[^0-9]+'
                                    ,2
                                ) as reg_right
                                ,regexp_extract(
                                    reg1
                                    ,'[^0-9]*?([1-9][0-9])(岁|周岁)?(及)?(以下|以内|内)'
                                    ,1
                                ) as reg_max
                                ,regexp_extract(reg1,'[^0-9]+([1-9][0-9])(岁|周岁)?左右',1) as reg_mid
                                ,regexp_extract(reg1,'[^0-9]+([1-9][0-9])(岁|周岁)?(及)?以上',1) as reg_min
                                ,regexp_extract(reg1,'不超过([1-9][0-9])(岁|周岁)',1) as reg_max_
                                ,regexp_extract(reg1,'年满([1-9][0-9])(岁|周岁)',1) as reg_min_
                        from    (
                                    select  jd_no
                                            ,regexp_extract(job_description,'.{0,20}([1-9][0-9]岁|年龄).{0,20}',0) as reg1
                                    from    zhaopin_round2_jd
                                ) t1
                    ) t2
        ) p2
on      p1.jd_no = p2.jd_no
join    (    --工作经验
            select  jd_no
                    ,case    when reg3 != '' then 0
                             when reg1 in ('10','十') or reg2 in ('10','十') then 4
                             when reg1 in ('5','6','7','8','9','五','六','七','八','九') or reg2 in ('5','6','7','8','9','五','六','七','八','九') then 3
                             when reg1 in ('3','4','三','四') or reg2 in ('3','4','三','四') then 2
                             when reg1 in ('1','2','一','二','半') or reg2 in ('1','2','一','二','半') then 1
                             else -1
                     end as min_years_2
            from    (
                        select  jd_no
                                ,regexp_extract(
                                    job_description
                                    ,'([0-9]|10|一|二|三|四|五|六|七|八|九|十)年.*?(工作经验|工作经历|经验|工作年限)'
                                    ,1
                                ) as reg1
                                ,regexp_extract(
                                    job_description
                                    ,'(工作经验|工作经历|经验|工作年限).*?([0-9]|10|一|二|三|四|五|六|七|八|九|十)年'
                                    ,2
                                ) as reg2
                                ,regexp_extract(
                                    concat(job_description,key,jd_title)
                                    ,'(不限经验|应(往)届|经验不限|[2019|2020]届|无需?经验[^者勿])'
                                    ,0
                                ) as reg3
                        from    zhaopin_round2_jd
                    ) t1
        ) p3
on      p1.jd_no = p3.jd_no
-- join    (    --薪资信息
--             select  jd_no
--                     ,least(
--                         coalesce(if(yuan_left<yuan_right,yuan_left,null),1000000)
--                         ,coalesce(if(qian_left<qian_right,qian_left,null),1000000)
--                         ,coalesce(if(wan_left<wan_right,wan_left,null),1000000)
--                         ,coalesce(yuan_single,1000000)
--                         ,coalesce(qian_single,1000000)
--                         ,coalesce(wan_single,1000000)
--                     ) as min_salary_2
--                     ,greatest(
--                         coalesce(if(yuan_left<yuan_right,yuan_right,null),-1)
--                         ,coalesce(if(qian_left<qian_right,qian_right,null),-1)
--                         ,coalesce(if(wan_left<wan_right,wan_right,null),-1)
--                         ,coalesce(yuan_single,-1)
--                         ,coalesce(qian_single,-1)
--                         ,coalesce(wan_single,-1)
--                     ) as max_salary_2
--             from    (
--                         select  jd_no
--                                 ,case    when reg1 = '' then -1
--                                          when cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',1) as bigint) > 4*max_salary and cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',1) as bigint) >=25000 then cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',1)/12 as bigint)
--                                          else cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',1) as bigint)
--                                  end as yuan_left
--                                 ,case    when reg1 = '' then -1
--                                          when cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',2) as bigint) > 4*max_salary and cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',2) as bigint) >=25000 then cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',2)/12 as bigint)
--                                          else cast(regexp_extract(replace(reg1,',',''),'([0-9]+)[^0-9]+([0-9]+)',2) as bigint)
--                                  end as yuan_right
--                                 ,case    when reg2 = '' then null
--                                          when cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',1) as bigint)*10000 > 4*max_salary and cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',1) as bigint)*10000 >=25000 then cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',1)*10000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',1)*10000 as bigint)
--                                  end as wan_left
--                                 ,case    when reg2 = '' then null
--                                          when cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',2)*10000 as bigint) > 4*max_salary and cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',2)*10000 as bigint) >=25000 then cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',2)*10000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg2,',',''),'([0-9][0-9,\.]*)[^0-9]+([0-9][0-9,\.]*)',2)*10000 as bigint)
--                                  end as wan_right
--                                 ,case    when reg3 = '' then null
--                                          when cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',1) as bigint)*1000 > 4*max_salary and cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',1) as bigint)*1000 >=25000 then cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',1)*1000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',1)*1000 as bigint)
--                                  end as qian_left
--                                 ,case    when reg3 = '' then null
--                                          when cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',2)*1000 as bigint) > 4*max_salary and cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',2)*1000 as bigint) >=25000 then cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',2)*1000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg3,',',''),'([0-9]+)[^0-9]+([0-9]+)',2)*1000 as bigint)
--                                  end as qian_right
--                                 ,case    when reg4 = '' then null
--                                          when cast(regexp_extract(replace(reg4,',',''),'([0-9]+)',1) as bigint) > 4*max_salary and cast(regexp_extract(replace(reg4,',',''),'([0-9]+)',1) as bigint) >=25000 then cast(regexp_extract(replace(reg4,',',''),'([0-9]+)',1)/12 as bigint)
--                                          else cast(regexp_extract(replace(reg4,',',''),'([0-9]+)',1) as bigint)
--                                  end as yuan_single
--                                 ,case    when reg5 = '' then null
--                                          when cast(regexp_extract(replace(reg5,',',''),'([0-9][0-9,\.]*)',1) as bigint)*10000 > 4*max_salary and cast(regexp_extract(replace(reg5,',',''),'([0-9][0-9,\.]*)',1) as bigint)*10000 >=25000 then cast(regexp_extract(replace(reg5,',',''),'([0-9][0-9,\.]*)',1)*10000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg5,',',''),'([0-9][0-9,\.]*)',1)*10000 as bigint)
--                                  end as wan_single
--                                 ,case    when reg6 = '' then null
--                                          when cast(regexp_extract(replace(reg6,',',''),'([0-9]+)',1) as bigint)*1000 > 4*max_salary and cast(regexp_extract(replace(reg6,',',''),'([0-9]+)',1) as bigint)*1000 >=25000 then cast(regexp_extract(replace(reg6,',',''),'([0-9]+)',1)*1000/12 as bigint)
--                                          else cast(regexp_extract(replace(reg6,',',''),'([0-9]+)',1)*1000 as bigint)
--                                  end as qian_single
--                         from    (
--                                     select  jd_no
--                                             ,max_salary
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'[1-9][0-9,]{1,5}00元?(～+|~+|-+|—+|到|/|至)[1-9][0-9,]{1,5}00元?'
--                                                 ,0
--                                             ) as reg1
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'[0-9][0-9,\.]{0,5}(万|w)?(～+|~+|-+|—+|到|/|至)[0-9][0-9,\.]{0,5}(万|w|W)'
--                                                 ,0
--                                             ) as reg2
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'[1-9][0-9,]{0,5}(k|K|千)?(～+|~+|-+|—+|到|/|至)[0-9][0-9,\.]{0,5}(k|K|千)'
--                                                 ,0
--                                             ) as reg3
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'(底薪|月薪|月收入|工资|薪水|报酬|年薪|年收入|待遇|收入|试用期|转正后)(:|：|>|=)?([1-9][0-9,]{1,5}00)元?'
--                                                 ,0
--                                             ) as reg4
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'(底薪|月薪|月收入|工资|薪水|报酬|年薪|年收入|待遇|收入|试用期|转正后)(:|：|>|=)?[0-9][0-9,\.]{0,5}(万|w)'
--                                                 ,0
--                                             ) as reg5
--                                             ,regexp_extract(
--                                                 concat(jd_title,key,job_description)
--                                                 ,'(底薪|月薪|月收入|工资|薪水|报酬|年薪|年收入|待遇|收入|试用期|转正后)(:|：|>|=)?[1-9][0-9,]{0,5}(k|K|千)'
--                                                 ,0
--                                             ) as reg6
--                                     from    zhaopin_round2_jd
--                                 ) t1
--                     ) t2
--         ) p4
-- on      p1.jd_no = p4.jd_no
;
