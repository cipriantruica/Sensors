create or replace TYPE num_array_sgbd IS table OF number(8)
/

--drop type num_array
--/

create or replace package gh_analytics
as
 
  type record_struct is record (
      greenhouse greenhouses.name%type,
      year number, 
      month number, 
      day number, 
      hour number, 
      sensor_type sensor_types.description%type,
      avg_value number(10,2),
      sum_value number(10,2),
      count_value number(10,2),
      min_value number(10,2),
      max_value number(10,2),
      stddev_value number(10,2),
      variance_value number(10,2),
      unit sensor_units.type%type
  );
  
  procedure analytics_by_hour(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_hour_rollup(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_hour_cube(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  
  procedure analytics_by_day(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_day_rollup(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_day_cube(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  
  procedure analytics_by_month(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_month_rollup(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  procedure analytics_by_month_cube(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);
  
  procedure analytics_by_year(c_data OUT sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp);

end gh_analytics;
/

create or replace package body gh_analytics
as
  procedure analytics_by_hour(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin

    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2 ) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.type
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst 
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) 
        and vst.id in (select * from table(v_id_sensors)) 
        and s.registered_time between start_date and end_date
      group by gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time),
        vst.DESCRIPTION, su.type
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time);
        
  end analytics_by_hour;
  
  procedure analytics_by_hour_cube(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2 ) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst 
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        cube(extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time);
  end analytics_by_hour_cube;
 
  procedure analytics_by_hour_rollup(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2 ) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst              
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        rollup(extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time), extract(hour from s.registered_time);

  end analytics_by_hour_rollup;
 
  procedure analytics_by_day(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit           
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time);

  end analytics_by_day;

  procedure analytics_by_day_cube(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit           
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        cube(extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time);

  end analytics_by_day_cube;
  
  procedure analytics_by_day_rollup(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst              
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit          
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        rollup(extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time), extract(day from s.registered_time);

  end analytics_by_day_rollup;
  
  procedure analytics_by_month(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst              inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit           on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time);

  end analytics_by_month;
  
  procedure analytics_by_month_cube(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst              
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit           
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        cube(extract(year from s.registered_time), extract(month from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time);

  end analytics_by_month_cube;
 
  procedure analytics_by_month_rollup(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time), extract(month from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        rollup(extract(year from s.registered_time), extract(month from s.registered_time)),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time), extract(month from s.registered_time);

  end analytics_by_month_rollup;
 
  
  procedure analytics_by_year(c_data out sys_refcursor, v_id_greenhouses IN OUT NUM_ARRAY, v_id_sensors IN OUT NUM_ARRAY, start_date IN OUT timestamp, end_date IN OUT timestamp)
  is
  begin
    
    if v_id_greenhouses.count = 0 or v_id_greenhouses is null then
      select id bulk collect into v_id_greenhouses from greenhouses;
    end if;
    
    if v_id_sensors.count = 0 or v_id_sensors is null then
      select id bulk collect into v_id_sensors from sensor_types;
    end if;
        
    if start_date is null then
        select min(registered_time) into start_date from sensors;
    end if;
    
    if end_date is null then
        select max(registered_time) into end_date from sensors;
    end if;
    
    open c_data for
      select 
        gh.NAME GreenHouse,
        extract(year from s.registered_time),
        vst.DESCRIPTION sensor_type,
        round(avg(s.SENSOR_VALUE), 2) avg_value,
        round(sum(s.SENSOR_VALUE), 2) sum_value,
        round(count(s.SENSOR_VALUE), 2) count_value,
        round(min(s.SENSOR_VALUE), 2) min_value,
        round(max(s.SENSOR_VALUE), 2) max_value,
        round(stddev(s.SENSOR_VALUE), 2) stddev_value,
        round(variance(s.SENSOR_VALUE), 2) variance_value,
        su.TYPE
      from
        sensors s
        inner join GREENHOUSES gh on gh.id = s.ID_GREENHOUSE
        inner join SENSOR_TYPES vst              
            inner join SENSOR_UNITS su on su.id = vst.id_sensor_unit           
          on vst.ID = s.ID_SENSOR_TYPE
      where 
        gh.id in (select * from table(v_id_greenhouses)) and 
        vst.id in (select * from table(v_id_sensors)) and
        s.registered_time between start_date and end_date
      group by gh.NAME,
        extract(year from s.registered_time),
        vst.DESCRIPTION, su.TYPE
      order by vst.DESCRIPTION, gh.NAME,
        extract(year from s.registered_time);
  end analytics_by_year;
end gh_analytics;
/