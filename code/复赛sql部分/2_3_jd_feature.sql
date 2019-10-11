--odps sql
--********************************************************************--
--author:hydantess
--create time:2019-08-28 20:11:41
--********************************************************************--
drop table if exists zhaopin_round2_jd_p ;

create table if not exists zhaopin_round2_jd_p as
select  p1.jd_no
        ,coalesce(city_id,-1) as jd_city_id
        ,coalesce(jd_type_id,-1) as jd_type_id
        ,cast(require_nums as bigint ) as require_nums
        ,case when int((max_salary+min_salary)/2) = 0 then -1
              when int((max_salary+min_salary)/2) <1000 then 0
              when int((max_salary+min_salary)/2) <2000 then 1
              when int((max_salary+min_salary)/2) <4000 then 2
              when int((max_salary+min_salary)/2) <6000 then 3
              when int((max_salary+min_salary)/2) <8000 then 4
              when int((max_salary+min_salary)/2) <10000 then 5
              when int((max_salary+min_salary)/2) <15000 then 6
              when int((max_salary+min_salary)/2) <25000 then 7
              else 8 end as avg_salary
        ,if(regexp_extract(concat(job_description,key,jd_title),'[^不|无需](出差)',0)!='',1,cast(is_travel as bigint )) as is_travel
        ,case   when  min_edu_level_2 !=-1 then min_edu_level_2
                when trim(min_edu_level) in ('硕士', '博士','MBA', 'EMBA') then 4
                 when trim(min_edu_level) in ('本科') then 3
                 when trim(min_edu_level) in ('大专') then 2
                 when trim(min_edu_level) in ('中专','中技','高中','初中') then 1
                 else -1
         end as min_edu_level
        ,datediff(
            to_date(end_date,'yyyymmdd')
            ,to_date(start_date,'yyyymmdd')
            ,'dd'
        ) as jd_last_days
        ,if(datediff(to_date('20190813','yyyymmdd'),to_date(start_date,'yyyymmdd')
            ,'dd'
        )<0,0,datediff(to_date('20190813','yyyymmdd'),to_date(start_date,'yyyymmdd')
            ,'dd'
        )) as jd_publish_days
        ,datediff(to_date(end_date,'yyyymmdd')
            ,to_date('20190813','yyyymmdd'),'dd'
        ) as jd_end_days
        ,datepart(to_date(start_date,'yyyymmdd')
            ,'dd'
        ) as jd_begin_day
        ,datepart(to_date(start_date,'yyyymmdd')
            ,'mm'
        ) as jd_begin_month
        ,case   when  min_years_2 !=-1 then min_years_2
                when min_years in (0) then 0
                 when min_years in (103,1,2) then 1
                 when min_years in (305,399,3) then 2
                 when min_years in (510,8,899,5) then 3
                 when min_years in (1099) then 4
                 else -1
         end as min_years
        ,age_1
        ,age_2
        ,length(job_description) as job_description_length
        ,count(p1.jd_no) over(partition by job_description) as sam_job_description_cnt
        ,regexp_count(job_description,'([a-zA-Z])') as job_description_letter_cnt
        ,if(length(job_description)>0,regexp_count(job_description,'([a-zA-Z])')/length(job_description),-1) as job_description_letter_ratio
        ,regexp_count(job_description,'([0-9])') as job_description_number_cnt
        ,if(length(job_description)>0,regexp_count(job_description,'([0-9])')/length(job_description),-1) as job_description_number_ratio
        ,length(jd_title) as jd_title_length
        ,regexp_count(jd_title,'([a-zA-Z])') as jd_title_letter_cnt
        ,if(length(jd_title)>0,regexp_count(jd_title,'([a-zA-Z])')/length(jd_title),-1) as jd_title_letter_ratio
        ,regexp_count(jd_title,'([0-9])') as jd_title_number_cnt
        ,if(length(jd_title)>0,regexp_count(jd_title,'([0-9])')/length(jd_title),-1) as jd_title_number_ratio
        ,regexp_count(
            job_description
            ,'(1|十)?([0-9]|一|二|三|四|五|六|七|八|九|十)[,.、)）]'
        ) as requirement_cnt
from    zhaopin_round2_jd p1
left outer join zhaopin_round2_city p2
on      p1.city = p2.city
left outer join zhaopin_round2_jd_type p3
on      p1.jd_sub_type = p3.jd_type
join    zhaopin_round2_jd_r p4
on      p1.jd_no = p4.jd_no
;