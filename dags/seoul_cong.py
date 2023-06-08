from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.models.variable import Variable
import pendulum

from urllib import parse
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs

API_KEY = Variable.get("SEOUL_API_KEY")
HQL_PATH = Variable.get("SEOUL_HQL_PATH")

local_tz = pendulum.timezone("Asia/Seoul")

place_cate = {'고궁·문화유산':['경복궁·서촌마을', '광화문·덕수궁', '창덕궁·종묘'],
        '관광특구' : ['강남 MICE 관광특구','동대문 관광특구', '명동 관광특구', '이태원 관광특구', '잠실 관광특구', 
                        '종로·청계 관광특구', '홍대 관광특구'],
        '공원':['국립중앙박물관·용산가족공원', '남산공원', '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲'
                ,'서울대공원', '서울숲공원', '월드컵공원', '이촌한강공원', '잠실종합운동장', '잠실한강공원'],
        '발달상권':['가로수길','낙산공원·이화마을', '노량진', '북촌한옥마을', '성수카페거리', '수유리 먹자골목', '쌍문동 맛집거리', 
                    '압구정로데오거리', '여의도', '영등포 타임스퀘어', '인사동·익선동', '창동 신경제 중심지', 'DMC(디지털미디어시티)'],
        '인구밀집지역':['가산디지털단지역','강남역','건대입구역','고속터미널역','교대역','구로디지털단지역','서울역','선릉역',
                        '신도림역' ,'신림역','신촌·이대역','왕십리역','역삼역','연신내역','용산역']}

place = place_cate.values()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1, tzinfo=local_tz),
    'retries': 0,
}
test_dag = DAG(
    'Get_Place_Cong',
    default_args=default_args,
    schedule="* */10 * * *"
)

DB_one={}

def get_place_list(*place_list):
    for place in place_list:
        DB_one[place]=get_api(place)

def get_api(place):
    path = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/citydata/1/5/'
    url=path+parse.quote(place)

    result = urlopen(url)  #7
    data = bs(result, 'lxml-xml')  #8
    AREA_NM=data.AREA_NM.string.strip()

    LIVE_PPLTN_STTS=data.find('LIVE_PPLTN_STTS').LIVE_PPLTN_STTS #인구
    SUB_STTS = data.find('SUB_STTS').SUB_STTS #지하철
    ROAD = data.find('AVG_ROAD_DATA') #도로소통 평균
    WEATHER_STTS = data.find('WEATHER_STTS').WEATHER_STTS #날씨
    PRK_STTS=data.find('PRK_STTS') #주차장
    FCST24 = data.find('FCST24HOURS') #날씨 예보

    live_arr = [
                AREA_NM,
                LIVE_PPLTN_STTS.AREA_CONGEST_LVL.string.strip(),
                LIVE_PPLTN_STTS.AREA_CONGEST_MSG.string.strip(),
                LIVE_PPLTN_STTS.AREA_PPLTN_MIN.string.strip(),
                LIVE_PPLTN_STTS.AREA_PPLTN_MAX.string.strip(),
                LIVE_PPLTN_STTS.PPLTN_TIME.string.strip()
    ]

    road_arr = [
                AREA_NM,
                ROAD.ROAD_MSG.text,
                ROAD.ROAD_TRAFFIC_IDX.text,
                ROAD.ROAD_TRFFIC_TIME.text,
                ROAD.ROAD_TRAFFIC_SPD.text
    ]

    sub_arr = [
                AREA_NM,
                SUB_STTS.SUB_STN_NM.string.strip(),
                SUB_STTS.SUB_STN_LINE.string.strip(),
                SUB_STTS.SUB_STN_RADDR.string.strip(),
                SUB_STTS.SUB_STN_JIBUN.string.strip(),
                SUB_STTS.SUB_STN_X.string.strip(),
                SUB_STTS.SUB_STN_Y.string.strip()
    ]

    wtr_arr = [
                AREA_NM,
                WEATHER_STTS.WEATHER_TIME.string.strip(),
                WEATHER_STTS.TEMP.string.strip(),
                WEATHER_STTS.SENSIBLE_TEMP.string.strip(),
                WEATHER_STTS.MAX_TEMP.string.strip(),
                WEATHER_STTS.MIN_TEMP.string.strip(),
                WEATHER_STTS.PRECIPITATION.string.strip(),
                WEATHER_STTS.PRECPT_TYPE.string.strip(),
                WEATHER_STTS.PCP_MSG.string.strip(),
                WEATHER_STTS.UV_INDEX.string.strip(),
                WEATHER_STTS.UV_MSG.string.strip(),
                WEATHER_STTS.PM25_INDEX.string.strip(),
                WEATHER_STTS.PM25.string.strip(),
                WEATHER_STTS.PM10_INDEX.string.strip(),
                WEATHER_STTS.PM10.string.strip(),
                WEATHER_STTS.AIR_IDX.string.strip(),
                WEATHER_STTS.AIR_IDX_MVL.string.strip(),
                WEATHER_STTS.AIR_IDX_MAIN.string.strip(),
                WEATHER_STTS.AIR_MSG.string.strip()
        ]

    prk_arr = []
    for i in PRK_STTS:
        tmp = [
                AREA_NM,
                i.PRK_NM.text,
                i.PRK_CD.text,
                i.CPCTY.text,
                i.CUR_PRK_CNT.text,
                i.CUR_PRK_TIME.text,
                i.CUR_PRK_YN.text,
                i.PAY_YN.text,
                i.RATES.text,
                i.TIME_RATES.text,
                i.ADDRESS.text,
                i.ROAD_ADDR.text,
                i.LNG.text,
                i.LAT.text
            ]
        prk_arr.append(tmp)

    fcst24_arr=[]
    for i in FCST24 :
        tmp = [
                AREA_NM,
                i.FCST_DT.text,
                i.TEMP.text,
                i.PRECIPITATION.text,
                i.PRECPT_TYPE.text,
                i.RAIN_CHANCE.text,  
                i.SKY_STTS.text,
            ]
        fcst24_arr.append(tmp)
    
    result = {
        'live':live_arr,
        'road':road_arr,
        'sub':sub_arr,
        'wtr':wtr_arr,
        'prk':park_arr,
    }

    return result


place_cate = {'고궁·문화유산':['경복궁·서촌마을', '광화문·덕수궁', '창덕궁·종묘'],
        '관광특구' : ['강남 MICE 관광특구','동대문 관광특구', '명동 관광특구', '이태원 관광특구', '잠실 관광특구', 
                        '종로·청계 관광특구', '홍대 관광특구'],
        '공원':['국립중앙박물관·용산가족공원', '남산공원', '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲'
                ,'서울대공원', '서울숲공원', '월드컵공원', '이촌한강공원', '잠실종합운동장', '잠실한강공원'],
        '발달상권':['가로수길','낙산공원·이화마을', '노량진', '북촌한옥마을', '성수카페거리', '수유리 먹자골목', '쌍문동 맛집거리', 
                    '압구정로데오거리', '여의도', '영등포 타임스퀘어', '인사동·익선동', '창동 신경제 중심지', 'DMC(디지털미디어시티)'],
        '인구밀집지역':['가산디지털단지역','강남역','건대입구역','고속터미널역','교대역','구로디지털단지역','서울역','선릉역',
                        '신도림역' ,'신림역','신촌·이대역','왕십리역','역삼역','연신내역','용산역']}

# Define the BashOperator task
init_tb = BashOperator(
    task_id='Init.DBtable',
    bash_command=f"hive -f {HQL_PATH}/init_db.hql",
    dag=test_dag
)                       

t1 = PythonOperator(task_id='Get.api_고궁문화유산',
                    python_callable=get_place_list,
                    op_args=place_cate['고궁·문화유산'],
                    dag=test_dag)

t2 = PythonOperator(task_id='Get.api_관광특구',
                    python_callable=get_place_list,
                    op_args=place_cate['관광특구'],
                    dag=test_dag)


t3 = PythonOperator(task_id='Get.api_공원',
                    python_callable=get_place_list,
                    op_args=place_cate['공원'],
                    dag=test_dag)


t4 = PythonOperator(task_id='Get.api_발달상권',
                    python_callable=get_place_list,
                    op_args=place_cate['발달상권'],
                    dag=test_dag)

t5 = PythonOperator(task_id='Get.api_인구밀집지역',
                    python_callable=get_place_list,
                    op_args=place_cate['인구밀집지역'],
                    dag=test_dag)


check_data = BashOperator(
    task_id='Check.mergedTable',
    bash_command=f"echo 'Completion of data collection for {len(DB_one)} place '",
    dag=test_dag
)


hql= '''
    SHOW TABLE cong_live;
    '''

h1 = HiveOperator(
    task_id='HiveOperator_test',
    hql=hql,
    hive_cli_conn_id='hive_cli_connect',
    run_as_owner=True,
    dag=test_dag,
)

init_tb >> [t1,t2,t3,t4,t5] >> check_data  >>h1








