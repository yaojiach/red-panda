SQL_NUM_SLICES = 'select count(1) from stv_slices'

SQL_LOAD_ERRORS = 'select * from stl_load_errors order by starttime desc'

SQL_TABLE_INFO = """\
SELECT TRIM(pgn.nspname) AS SCHEMA,
TRIM(a.name) AS TABLE,
id AS TableId,
decode(pgc.reldiststyle, 0, 'EVEN', 1, det.distkey, 8, 'ALL') AS DistKey,
decode(pgc.reldiststyle,8, NULL, dist_ratio.ratio::DECIMAL(20,4)) AS Skew,
det.head_sort AS "SortKey",
det.n_sortkeys AS "#SKs",
CASE WHEN pgc.reldiststyle = 8 THEN a.rows_all_dist ELSE a.rows END AS rows,
b.mbytes,
decode(det.max_enc, 0, 'N', 'Y') AS Enc, 
det.pct_enc, 
decode(b.mbytes, 0, 0,((b.mbytes/part.total::DECIMAL)*100)::DECIMAL(20,2)) AS pct_of_total,
CASE 
    WHEN a.rows = 0 
        THEN NULL 
    ELSE 
        CASE 
            WHEN pgc.reldiststyle = 8 
                THEN ((a.rows_all_dist - pgc.reltuples)::DECIMAL(20,3) / a.rows_all_dist::DECIMAL(20,3)*100)::DECIMAL(20,2)
            ELSE ((a.rows - pgc.reltuples)::DECIMAL(20,3) / a.rows::DECIMAL(20,3)*100)::DECIMAL(20,2) 
    END 
END AS pct_stats_off,
CASE 
    WHEN pgc.reldiststyle = 8 
        THEN decode(det.n_sortkeys, 0, NULL, DECODE(a.rows_all_dist, 0, 0, (a.unsorted_rows_all_dist::DECIMAL(32)/a.rows_all_dist)*100))::DECIMAL(20,2)
    ELSE decode(det.n_sortkeys, 0, NULL, DECODE(a.rows, 0, 0, (a.unsorted_rows::DECIMAL(32)/a.rows)*100))::DECIMAL(20,2) 
END AS pct_unsorted
FROM (
    SELECT db_id,
    id,
    name,
    SUM(ROWS) AS ROWS,
    MAX(ROWS) AS rows_all_dist,
    SUM(ROWS) - SUM(sorted_rows) AS unsorted_rows,
    MAX(ROWS) - MAX(sorted_rows) AS unsorted_rows_all_dist
    FROM stv_tbl_perm a
    GROUP BY db_id,
    id,
    name
) AS a
JOIN pg_class AS pgc ON pgc.oid = a.id
JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
LEFT OUTER JOIN (
    SELECT tbl, COUNT(*) AS mbytes FROM stv_blocklist GROUP BY tbl
) b ON a.id = b.tbl
INNER JOIN (
    SELECT attrelid,
    MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey",
    MIN(CASE attsortkeyord WHEN 1 THEN attname ELSE NULL END) AS head_sort,
    MAX(attsortkeyord) AS n_sortkeys,
    MAX(attencodingtype) AS max_enc,
    SUM(case when attencodingtype <> 0 then 1 else 0 end)::DECIMAL(20,3)/COUNT(attencodingtype)::DECIMAL(20,3) * 100.00 as pct_enc
    FROM pg_attribute
    GROUP BY 1
) AS det ON det.attrelid = a.id
INNER JOIN (
    SELECT tbl,
    MAX(Mbytes)::DECIMAL(32) /MIN(Mbytes) AS ratio
    FROM (
        SELECT tbl,
        TRIM(name) AS name,
        slice,
        COUNT(*) AS Mbytes
        FROM svv_diskusage
        GROUP BY tbl,
        name,
        slice
    )
    GROUP BY tbl,
    name
) AS dist_ratio ON a.id = dist_ratio.tbl
JOIN (
    SELECT SUM(capacity) AS total
    FROM stv_partitions
    WHERE part_begin = 0
) AS part ON 1 = 1
WHERE mbytes IS NOT NULL
AND   pgc.relowner > 1
ORDER BY mbytes DESC
"""

SQL_RUNNING_INFO = """\
select trim(u.usename) as user, 
s.pid, 
q.xid,
q.query,
q.service_class as "q", 
q.slot_count as slt, 
date_trunc('second',q.wlm_start_time) as start,
decode(trim(q.state), 'Running', 'Run', 'QueuedWaiting', 'Queue', 'Returning', 'Return', trim(q.state)) as state, 
q.queue_Time/1000000 as q_sec, 
q.exec_time/1000000 as exe_sec, 
m.cpu_time/1000000 cpu_sec, 
m.blocks_read read_mb, 
decode(m.blocks_to_disk,-1,null,m.blocks_to_disk) spill_mb, 
m2.rows as ret_rows, m3.rows as NL_rows,
substring(replace(nvl(qrytext_cur.text,trim(translate(s.text,chr(10)||chr(13)||chr(9) ,''))),'\\n',' '), 1, 90) as sql,
trim(decode(event&1,1,'SK ','') || decode(event&2,2,'Del ','') || decode(event&4,4,'NL ','') || decode(event&8,8,'Dist ','') || decode(event&16,16,'Bcast ','') || decode(event&32,32,'Stats ','')) as Alert
from stv_wlm_query_state q 
left outer join stl_querytext s on (s.query=q.query and sequence = 0)
left outer join stv_query_metrics m on ( q.query = m.query and m.segment=-1 and m.step=-1 )
left outer join stv_query_metrics m2 on ( q.query = m2.query and m2.step_type = 38 )
left outer join ( select query, sum(rows) as rows from stv_query_metrics m3 where step_type = 15 group by 1) as m3 on ( q.query = m3.query )
left outer join pg_user u on ( s.userid = u.usesysid )
LEFT OUTER JOIN (
    SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
    FROM stl_utilitytext ut
    WHERE sequence = 0
    AND upper(TEXT) like 'DECLARE%'
    GROUP BY text, ut.xid) qrytext_cur ON 
(q.xid = qrytext_cur.xid)
left outer join (
    select query,
    sum(decode(
        trim(split_part(event,':',1)), 'Very selective query filter', 
        1, 'Scanned a large number of deleted rows', 
        2, 'Nested Loop Join in the query plan',
        4,'Distributed a large number of rows across the network',
        8,'Broadcasted a large number of rows across the network',
        16,'Missing query planner statistics',32,0)
    ) as event 
    from STL_ALERT_EVENT_LOG 
    where event_time >=  dateadd(hour, -8, current_Date) 
    group by query  
) as alrt 
on alrt.query = q.query
order by q.service_class,q.exec_time desc, q.wlm_start_time
"""
