-- Q1
SELECT ci.INFRASTRUCTURE_ID 
FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci 
WHERE sen.id=ci.SENSOR_ID AND sen.id=?

-- Q2
SELECT sen.name 
FROM SENSOR sen, SENSOR_TYPE st, COVERAGE_INFRASTRUCTURE ci 
WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(?)

-- Q3
SELECT timeStamp, %s 
FROM %sObservation  
WHERE timestamp>? AND timestamp<? AND SENSOR_ID=?

-- Q4
SELECT timeStamp, %s 
FROM %Observation 
WHERE timestamp>? AND timestamp<? AND SENSOR_ID=ANY(?)

-- Q5
SELECT timeStamp, %s FROM %sOBSERVATION o 
WHERE timestamp>? AND timestamp<? AND %s>=? AND %s<=?

-- Q6
SELECT obs.sensor_id, avg(counts) 
FROM (SELECT sensor_id, date_trunc('day', timestamp), count(*) as counts 
      FROM %sObservation WHERE timestamp>? AND timestamp<? AND SENSOR_ID = ANY(?) 
      GROUP BY sensor_id, date_trunc('day', timestamp)) AS obs 
GROUP BY sensor_id

-- Q7
SELECT u.name 
FROM PRESENCE s1, PRESENCE s2, USERS u 
WHERE date_trunc('day', s1.timeStamp) = ? AND date_trunc('day', s2.timeStamp) = ? AND s1.semantic_entity_id = s2.semantic_entity_id 
AND s1.location = ? AND s2.location = ? AND s1.timeStamp < s2.timeStamp AND s1.semantic_entity_id = u.id 

-- Q8
SELECT u.name, s1.location 
FROM PRESENCE s1, PRESENCE s2, USERS u 
WHERE date_trunc('day', s1.timeStamp) = ? AND s2.timeStamp = s1.timeStamp AND s1.semantic_entity_id = ? 
AND s1.semantic_entity_id != s2.semantic_entity_id AND s2.semantic_entity_id = u.id AND s1.location = s2.location

-- Q9
SELECT Avg(timeSpent) as avgTimeSpent FROM 
	(SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent 
         FROM PRESENCE so, Infrastructure infra, Infrastructure_Type infraType 
         WHERE so.location = infra.id AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? AND so.semantic_entity_id = ? 
         GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay

-- Q10
SELECT infra.name, so.timeStamp, so.occupancy 
FROM OCCUPANCY so, INFRASTRUCTURE infra 
WHERE so.timeStamp > ? AND so.timeStamp < ? AND so.semantic_entity_id = infra.id 
ORDER BY so.semantic_entity_id, so.timeStamp
