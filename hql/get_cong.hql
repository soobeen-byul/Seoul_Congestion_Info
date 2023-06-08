DROP TABLE IF EXISTS cong_live;
DROP TABLE IF EXISTS cong_park;
DROP TABLE IF EXISTS cong_road;
DROP TABLE IF EXISTS cong_sub;
DROP TABLE IF EXISTS cong_wtr;
DROP TABLE IF EXISTS cong_fcst24wtr;


-- 인구 정보
CREATE TABLE cong_live(
	AREA_NM STRING,
	AREA_CONGEST_LVL STRING,
	AREA_CONGEST_MSG STRING,.
	AREA_PPLTN_MIN INT,
	AREA_PPLTN_MAX INT,
	PPLTN_TIME TIMESTAMP
	);

-- 주차장 정보
CREATE TABLE cong_prk(
	AREA_NM STRING,
	PRK_NM STRING,
	PRK_CD INT,
	CPCTY INT,
	CUR_PRK_CNT INT,
	CUR_PRK_TIME TIMESTAMP,
	CUR_PRK_YN CHAR,
	PAY_YN STIRNG,
	RATES INT,
	TIME RATES INT,
	ADD_RATES INT,
	ADD_TIME_RATES INT,
	ADDRESS STRING,
	ROAD_ADDR STRING,
	LNG DOUBLE,
	LAT DOUBLE
	);

-- 도로 소통 평균 정보
CREATE TABLE cong_road(
	AREA_NM STRING,
	ROAD_MSG STRING,
	ROAD_TRAFFIC_IDX STRING,
	ROAD_TRFFIC_TIME TIMESTAMP,
	ROAD_TRAFFIC_SPD INT
	);
	


-- 도로 소통 정보
-- CREATE TABLE cong_road(
	AREA_NM STRING,
	LINK_ID INT,
	ROAD_NM STRING,
	START_ND_CD INT,
	START_ND_NM STRING,
	START_ND_XY STRING,
	END_ND_CD INT,
	END_ND_NM STRING,
	END_ND_XY STRING,
	DIST INT,
	SPT FLOAT,
	IDX STRING,
	XYLIST SRINT
	);

-- 지하철 정보
CREATE TABLE cong_sub(
	AREA_NM STRING,
	SUB_STN_NM STRING,
	SUB_STN_LINE INT,
	SUB_STN_RADDR STRING,
	SUB_STN_JIBUN STRING,
	SUB_STN_X DOUBLE,
	SUB_STN_Y DOUBLE
	);

-- 날씨 정보
CREATE cong_wtr(
	AREA_NM STRING,
	WEATHER_TIME TIMESTAMP,
	TEMP FLOAT,
	SENSIBLE_TEMP FLOAT,
	MAX_TEMP FLOAT,
	MIN_TEMP FLOAT,
	PRECIPITATION STRING,
	PRECPT_TYPE STRING,
	PCP_MSG STRING,
	UV_INDEX STRING,
	UV_MSG STRING,
	PM25_INDEX STRING,
	PM25 INT,
	PM10_INDEX STRING,
	PM10 INT,
	AIR_IDX STRING,
	AIR_IDX_MVL FLOAT,
	AIR_IDX_MAIN STRING,
	AIR_MSG STRING
	);

-- 날씨 예보
CREATE cong_fcst24wtr(
	AREA_NM STRING,
	FACST_DT INT,
	TEMP INT,
	PRECIPATAION STRING,
	PRECPT_TYPE STRING,
	RAIN_CHANCE INT,
	SKY_STTS STRING
	);





	

