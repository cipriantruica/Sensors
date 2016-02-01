set serveroutput on
set verify off
declare
  type r_c is ref cursor;
  mc r_c;
  elem GH_ANALYTICS.record_struct;
  a1 num_array := new num_array(2);
  a2 num_array := new num_array(3,5);
  t1 timestamp;-- := to_date('01-01-2015 00:00:00', 'DD-MM-YYYY HH24:Mi:SS');
  t2 timestamp;-- := to_date('18-09-2016 00:00:00', 'DD-MM-YYYY HH24:Mi:SS');
begin
  
--  select id bulk collect into a1 from greenhouses where id=1007;
--  select id bulk collect into a2 from sensor_types where id in (1, 4);

  dbms_output.put_line('Group by hour');
  GH_ANALYTICS.analytics_by_hour(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
  loop
    fetch mc into elem;
    exit when mc%notfound;
    dbms_output.put_line(
      elem.greenhouse || ' ' || 
      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' || nvl(elem.day, 0)  || ' ' || nvl(elem.hour, 0)  || ' ' ||
      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
  end loop;
  if mc%isopen then
    close mc;
  end if;
  
--  dbms_output.put_line('Group by hour rollup');
--  GH_ANALYTICS.analytics_by_hour_rollup(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, -1) || ' ' || nvl(elem.month, -1) || ' ' || nvl(elem.day, -1)  || ' ' || nvl(elem.hour, -1)  || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by cube');
--  GH_ANALYTICS.analytics_by_hour_cube(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' || nvl(elem.day, 0)  || ' ' || nvl(elem.hour, 0)  || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by day');
--  GH_ANALYTICS.analytics_by_day(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.day, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' || nvl(elem.day, 0)  || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by day rollup');
--  GH_ANALYTICS.analytics_by_day_rollup(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.day, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' || nvl(elem.day, 0)  || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by day cube');
--  GH_ANALYTICS.analytics_by_day_cube(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.day, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' || nvl(elem.day, 0)  || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
  dbms_output.put_line('Group by month');
  GH_ANALYTICS.analytics_by_month(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
  loop
    fetch mc into elem.greenhouse, elem.year, elem.month, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
    exit when mc%notfound;
    dbms_output.put_line(
      elem.greenhouse || ' ' || 
      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' ||
      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
  end loop;
  if mc%isopen then
    close mc;
  end if;
--  
--  dbms_output.put_line('Group by month rollup');
--  GH_ANALYTICS.analytics_by_month_rollup(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by month cube');
--  GH_ANALYTICS.analytics_by_month_cube(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' || nvl(elem.month, 0) || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
--  
--  dbms_output.put_line('Group by year');
--  GH_ANALYTICS.analytics_by_year(c_data => mc, v_id_greenhouses => a1, v_id_sensors => a2, start_date => t1, end_date => t2);
--  loop
--    fetch mc into elem.greenhouse, elem.year, elem.month, elem.sensor_type, elem.avg_value, elem.sum_value, elem.count_value, elem.min_value, elem.max_value, elem.stddev_value, elem.variance_value, elem.unit;
--    exit when mc%notfound;
--    dbms_output.put_line(
--      elem.greenhouse || ' ' || 
--      nvl(elem.year, 0) || ' ' ||
--      elem.sensor_type || ' ' || elem.avg_value || ' ' || elem.min_value || ' '  || elem.max_value  || ' ' || elem.unit);
--  end loop;
--  if mc%isopen then
--    close mc;
--  end if;
  
--  dbms_output.put_line(t1|| ' '|| t2);
--  for i in a1.first .. a1.last
--  loop
--    dbms_output.put_line(a1(i));
--  end loop;
--  
--  for i in a2.first .. a2.last
--  loop
--    dbms_output.put_line(a2(i));
--  end loop;
end;
/


--declare
--  type x is table of number index by pls_integer;
--  y x;
--  type z is ref cursor;
--  q z;
--begin
--  select id bulk collect into y from GREENHOUSES;
--  
--  for i in y.first .. y.last
--  loop
--    SYS.DBMS_OUTPUT.PUT_LINE(y(i));
--  end loop;
--  
----  open q for
----    select * from GREENHOUSES where id in y;
--  
--end;
--/


select min(registered_time), max(registered_time) from sensors;
