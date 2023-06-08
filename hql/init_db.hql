
-- 테이블이 없을 경우 테이블 생성
-- 인구 정보
CREATE TABLE IF NOT EXIST cong_live(
	AREA_NM STRING,
	AREA_CONGEST_LVL STRING,
	AREA_CONGEST_MSG STRING,.
	AREA_PPLTN_MIN INT,
	AREA_PPLTN_MAX INT,
	PPLTN_TIME TIMESTAMP
	);

-- 주차장 정보
CREATE TABLE IF NOT EXIST cong_prk(
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
CREATE TABLE IF NOT EXIST cong_road(
	AREA_NM STRING,
	ROAD_MSG STRING,
	ROAD_TRAFFIC_IDX STRING,
	ROAD_TRFFIC_TIME TIMESTAMP,
	ROAD_TRAFFIC_SPD INT
	);

-- 지하철 정보
CREATE TABLE IF NOT EXIST cong_sub(
	AREA_NM STRING,
	SUB_STN_NM STRING,
	SUB_STN_LINE INT,
	SUB_STN_RADDR STRING,
	SUB_STN_JIBUN STRING,
	SUB_STN_X DOUBLE,
	SUB_STN_Y DOUBLE
	);

-- 날씨 정보
CREATE IF NOT EXIST cong_wtr(
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
CREATE IF NOT EXIST cong_fcst24wtr(
	AREA_NM STRING,
	FACST_DT INT,
	TEMP INT,
	PRECIPATAION STRING,
	PRECPT_TYPE STRING,
	RAIN_CHANCE INT,
	SKY_STTS STRING
	);


-- 테이블이 있을 경우 내용 삭제
DELETE TABLES IF EXISTS cong_live;
DELETE TABLES IF EXISTS cong_prk;
DELETE TABLES IF EXISTS cong_road;
DELETE TABLES IF EXISTS cong_sub;
DELETE TABLES IF EXISTS cong_wtr;
DELETE TABLES IF EXISTS cong_fcst24wtr;	

