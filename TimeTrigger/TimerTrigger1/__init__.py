import datetime
import logging
import azure.functions as func
import pymysql


def mysqlcon():

    connection = pymysql.Connect(host='hauzermysql.mysql.database.azure.com', user='mysa', password="HauzerSQL!", database='mysqldb', cursorclass=pymysql.cursors.DictCursor, autocommit=True, ssl={"fake_flag_to_enable_tls":True})

    return connection.cursor()

def collect_data():

    cursor = mysqlcon()

    # Read a single record

    sql = """
    
with delta_avg_table as
(
	SELECT *, 
			log_stepdate - (min(log_stepdate) over (partition by batchnumber order by id )) duration,
			act_temp_1 - (lag(act_temp_1,4,0) over (partition by batchnumber order by id )) delta_2m_4, 
			act_temp_1 - (lag(act_temp_1,8,0) over (partition by batchnumber order by id )) delta_4m_8,
			#act_temp_1 - (lag(act_temp_1,12,0) over (partition by batchnumber order by id )) delta_6m_12,
			AVG(act_temp_1) OVER (partition by batchnumber order by id ROWS BETWEEN CURRENT ROW and 4 following ) avg4,
			AVG(act_temp_1) OVER (partition by batchnumber order by id ROWS BETWEEN CURRENT ROW and 6 following) avg6,
			round(0.4*(count(*) over()),0) percrow, 
			row_number() over (partition by batchnumber order by id ) rownr,
            MIN(log_stepdate) OVER(PARTITION BY batchnumber) BatchStart,
			MAX(log_stepdate) OVER(PARTITION BY batchnumber) BatchEnd, 
			MIN(log_stepdate) OVER(PARTITION BY batchnumber,rcp_revid) BatchStepStart,
			MAX(log_stepdate) OVER(PARTITION BY batchnumber,rcp_revid) BatchStepEnd,
			TIMEDIFF(MAX(log_stepdate) OVER(PARTITION BY batchnumber), MIN(log_stepdate) OVER(PARTITION BY batchnumber)) as DurPBatch,
			TIMEDIFF(MAX(log_stepdate) OVER(PARTITION BY batchnumber,rcp_revid), MIN(log_stepdate) OVER(PARTITION BY batchnumber,rcp_revid)) as DurPBatchStep,
			SUM(bias_arccount) OVER (PARTITION BY batchnumber ORDER BY id) as CumulativeArcPBatch,
			SUM(bias_arccount) OVER (PARTITION BY batchnumber,rcp_revid ORDER BY id ) as CumulativeArcPBatchStep,
			AVG(act_temp_1) OVER(PARTITION BY batchnumber,rcp_revid) as AvgTemp1PBatchStep,
			MAX(act_temp_1) OVER(PARTITION BY batchnumber,rcp_revid) as MaxTemp1PBatchStep,
			LAST_VALUE(act_temp_1) OVER (PARTITION BY batchnumber,rcp_revid ORDER BY log_stepdate 
				RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as EndTemp1,
			AVG(act_temp_2) OVER(PARTITION BY batchnumber,rcp_revid) as AvgTemp2PBatchStep,
			MAX(act_temp_2) OVER(PARTITION BY batchnumber,rcp_revid) as MaxTemp2PBatchStep,
			LAST_VALUE(act_temp_2) OVER (PARTITION BY batchnumber,rcp_revid ORDER BY log_stepdate 
				RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as EndTemp2,
			AVG(baraton_mbar) OVER(PARTITION BY batchnumber,rcp_revid) as AvgBaratronPBatchStep,
			MAX(baraton_mbar) OVER(PARTITION BY batchnumber,rcp_revid) as MaxBaratronPBatchStep,
			MIN(penning_mbar) OVER(PARTITION BY batchnumber,rcp_revid) as MinPenningPBatchStep
	FROM kpidata
),
heat_up as
(
	select *, 
		#Get stabdard avg
		max(avg_temp) over(partition by batchnumber) max_avg_temp,  
        #Get avg delta 
		avg(delta_2m_4) over (partition by batchnumber order by id ROWS BETWEEN current row and 8 following) avg_delta_2m_4
		#avg(delta_4m_8) over (partition by batchnumber order by id ROWS BETWEEN current row and 10 following) avg_delta_4m_8
	from
		#Get avg from 70% amount of last rows
		((select *, avg(act_temp_1) over(partition by batchnumber) avg_temp
		from delta_avg_table u
		where rownr <=  percrow)
		union
		(select *, avg(act_temp_1) over(partition by batchnumber) avg_temp
		from delta_avg_table t
		where rownr >  percrow))a
	),
stable_heat_up as
(
    select *, 
        (max_avg_temp - avg4) diff_avg4, (max_avg_temp - avg6) diff_avg6,
        (case when (max_avg_temp - avg4) < 1 and (max_avg_temp - avg4) > -1  then 1 else 0 end) as stable_point_avg_4
    from heat_up
),
stable_heat_up2 as
(
    select *, min(id) over (partition by batchnumber,stable_point_avg_4 order by id ) min
    from stable_heat_up
),
stable_point as
(
    select *, max(min) over (partition by batchnumber ) min2
    from stable_heat_up2
),
final_table as
(
    select *, max(case when stable_point_avg_4=1 and id=min2 then duration else 0 end) over () as DurStable_ACT_Temp1
    from stable_point
)
select id, rcp_revid, batchnumber,recipe_name,steplogname,log_stepdate,rcp_seqallid,bias_arccount,act_temp_1,act_temp_2,pirani_a2_mbar,penning_mbar,baraton_mbar, BatchStart,BatchEnd,BatchStepStart,BatchStepEnd, DurPBatch,DurPBatchStep,CumulativeArcPBatch,CumulativeArcPBatchStep,AvgTemp1PBatchStep,MaxTemp1PBatchStep, DurStable_ACT_Temp1,EndTemp1,AvgTemp2PBatchStep,MaxTemp2PBatchStep,EndTemp2,AvgBaratronPBatchStep,MaxBaratronPBatchStep,MinPenningPBatchStep
from final_table

 """
    cursor.execute(sql)

    result = cursor.fetchall()

    print(result)

    return result
 

def insert(result):
    cursor = mysqlcon()
    for row in result:
       # print("--")
        print(row['BatchStart'])
        #print("--")
        query = "INSERT INTO kpi_calculated (`id`, `rcp_revid`, `batchnumber`, `recipe_name`, `steplogname`, `log_stepdate`, `rcp_seqallid`, `bias_arccount`, `act_temp_1`, `act_temp_2`, `pirani_a2_mbar`, `penning_mbar`, `baraton_mbar`, `BatchStart`, `BatchEnd`, `BatchStepStart`, `BatchStepEnd`, `DurPBatch`, `DurPBatchStep`, `CumulativeArcPBatch`, `CumulativeArcPBatchStep`, `AvgTemp1PBatchStep`, `MaxTemp1PBatchStep`, `DurStable_ACT_Temp1`, `EndTemp1`, `AvgTemp2PBatchStep`, `MaxTemp2PBatchStep`, `EndTemp2`, `AvgBaratronPBatchStep`, `MaxBaratronPBatchStep`, `MinPenningPBatchStep`) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"
        values = (row['id'],row['rcp_revid'],row['batchnumber'],row['recipe_name'],row['steplogname'],row['log_stepdate'],row['rcp_seqallid'],row['bias_arccount'],row['act_temp_1'],row['act_temp_2'],row['pirani_a2_mbar'],row['penning_mbar'],row['baraton_mbar'],row['BatchStart'],row['BatchEnd'],row['BatchStepStart'],row['BatchStepEnd'],row['DurPBatch'],row['DurPBatchStep'],row['CumulativeArcPBatch'],row['CumulativeArcPBatchStep'],row['AvgTemp1PBatchStep'],row['MaxTemp1PBatchStep'],row['DurStable_ACT_Temp1'],row['EndTemp1'],row['AvgTemp2PBatchStep'],row['MaxTemp2PBatchStep'],row['EndTemp2'],row['AvgBaratronPBatchStep'],row['MaxBaratronPBatchStep'],row['MinPenningPBatchStep']) 
        cursor.execute(query, values)


def deldata():
    cursor = mysqlcon()
    query = "delete from kpi_calculated"
    cursor.execute(query)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    deldata()
    x = collect_data()
    insert(x)