from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.models.variable import Variable
import pendulum
import pandas as pd

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

placeKey = [
            '경복궁·서촌마을', '광화문·덕수궁', '창덕궁·종묘','강남 MICE 관광특구', '동대문 관광특구', '명동 관광특구', 
            '이태원 관광특구', '잠실 관광특구', '종로·청계 관광특구', '홍대 관광특구','국립중앙박물관·용산가족공원', 
            '남산공원', '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲', '서울대공원', '서울숲공원', 
            '월드컵공원', '이촌한강공원', '잠실종합운동장', '잠실한강공원','가로수길', '낙산공원·이화마을', '노량진', 
            '북촌한옥마을', '성수카페거리', '수유리 먹자골목', '쌍문동 맛집거리', '압구정로데오거리', '여의도', '영등포 타임스퀘어', 
            '인사동·익선동', '창동 신경제 중심지', 'DMC(디지털미디어시티)','가산디지털단지역', '강남역', '건대입구역', '고속터미널역', 
            '교대역', '구로디지털단지역', '서울역', '선릉역', '신도림역', '신림역', '신촌·이대역', '왕십리역', '역삼역', '연신내역', 
            '용산역'
            ]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1, tzinfo=local_tz),
    'retries': 0,
}
test_dag = DAG(
    'tmp',
    default_args=default_args,
    schedule="* */30 * * *",
    user_defined_macros={'local_dt': lambda execution_date: execution_date.in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")},
)

# check_execute_task = BashOperator(
#     task_id='check.execute',
#     bash_command="""
#         echo "date                            => `date`"
#         echo "logical_date                    => {{logical_date}}"
#         echo "execution_date                  => {{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
#         echo "local_dt(execution_date)        => {{local_dt(execution_date)}}"
#         """,
#     dag = test_dag
#     )

def get_api_data(**context):
    DB_one={}
    for place in placeKey[:2]:
        print(place,'진행중')
        DB_one[place]=get_api(place)
        print('완료')

    return DB_one

def get_api(place):
    path = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/citydata/1/5/'
    url=path+parse.quote(place)

    result = urlopen(url)  #7
    data = bs(result, 'lxml-xml')  #8
    AREA_NM=data.AREA_NM.text

    LIVE_PPLTN_STTS=data.find('LIVE_PPLTN_STTS').LIVE_PPLTN_STTS #인구
    SUB_STTS = data.find('SUB_STTS') #지하철
    ROAD = data.find('AVG_ROAD_DATA') #도로소통 평균
    WEATHER_STTS = data.find('WEATHER_STTS').WEATHER_STTS #날씨
    PRK_STTS=data.find('PRK_STTS') #주차장
    FCST24 = data.find('FCST24HOURS') #날씨 예보

    live_arr = [
                AREA_NM,
                LIVE_PPLTN_STTS.AREA_CONGEST_LVL.text,
                LIVE_PPLTN_STTS.AREA_CONGEST_MSG.text,
                LIVE_PPLTN_STTS.AREA_PPLTN_MIN.text,
                LIVE_PPLTN_STTS.AREA_PPLTN_MAX.text,
                LIVE_PPLTN_STTS.PPLTN_TIME.text
    ]

    road_arr = [
                AREA_NM,
                ROAD.ROAD_MSG.text,
                ROAD.ROAD_TRAFFIC_IDX.text,
                ROAD.ROAD_TRFFIC_TIME.text,
                ROAD.ROAD_TRAFFIC_SPD.text
    ]
    if len(SUB_STTS) == 0 :
        sub_arr=[]
        print('지하철 정보가 없습니다')


    elif len(SUB_STTS) > 1:
      sub_arr=[]
      for i in SUB_STTS:
          tmp = [
                AREA_NM,
                i.SUB_STN_NM.text,
                i.SUB_STN_LINE.text,
                i.SUB_STN_RADDR.text,
                i.SUB_STN_JIBUN.text,
                i.SUB_STN_X.text,
                i.SUB_STN_Y.text
                ]
          sub_arr.append(tmp)

    else:
      SUB_STTS=SUB_STTS.SUB_STTS

      sub_arr = [
                  AREA_NM,
                  SUB_STTS.SUB_STN_NM.text,
                  SUB_STTS.SUB_STN_LINE.text,
                  SUB_STTS.SUB_STN_RADDR.text,
                  SUB_STTS.SUB_STN_JIBUN.text,
                  SUB_STTS.SUB_STN_X.text,
                  SUB_STTS.SUB_STN_Y.text
      ]

    wtr_arr = [
                AREA_NM,
                WEATHER_STTS.WEATHER_TIME.text,
                WEATHER_STTS.TEMP.text,
                WEATHER_STTS.SENSIBLE_TEMP.text,
                WEATHER_STTS.MAX_TEMP.text,
                WEATHER_STTS.MIN_TEMP.text,
                WEATHER_STTS.PRECIPITATION.text,
                WEATHER_STTS.PRECPT_TYPE.text,
                WEATHER_STTS.PCP_MSG.text,
                WEATHER_STTS.UV_INDEX.text,
                WEATHER_STTS.UV_MSG.text,
                WEATHER_STTS.PM25_INDEX.text,
                WEATHER_STTS.PM25.text,
                WEATHER_STTS.PM10_INDEX.text,
                WEATHER_STTS.PM10.text,
                WEATHER_STTS.AIR_IDX.text,
                WEATHER_STTS.AIR_IDX_MVL.text,
                WEATHER_STTS.AIR_IDX_MAIN.text,
                WEATHER_STTS.AIR_MSG.text
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
        'prk':prk_arr,
        'fcst24':fcst24_arr
    }

    return result


# Define the BashOperator task
init_tb = BashOperator(
    task_id='Init.DBtable',
    bash_command=f"hive -f {HQL_PATH}/init_table.hql",
    dag=test_dag
)                       

get_data = PythonOperator(task_id='Get.api_data',
                    python_callable=get_api_data,
                    dag=test_dag)



def print_data_size(**context):
    DB_one = context['task_instance'].xcom_pull(task_ids='Get.api_data')
    print(f"Completion of data collection for {len(DB_one)} place")

check_data = PythonOperator(
    task_id='Check.data',
    python_callable=print_data_size,
    dag=test_dag
)

def merge_data(**context):
    DB_one = context['task_instance'].xcom_pull(task_ids='Get.api_data')
    cong_live, cong_prk, cong_road, cong_sub, cong_wtr, cong_fcst24wtr = [],[],[],[],[],[]
    for place in placeKey[:2]:
        cong_live.append(DB_one[place]['live'])
        cong_road.append(DB_one[place]['road'])
        cong_wtr.append(DB_one[place]['wtr'])
        cong_prk.append(DB_one[place]['prk'])
        cong_fcst24wtr.append(DB_one[place]['fcst24'])

        if DB_one[place]['sub'] != [] :
            cong_sub.append(DB_one[place]['sub'])
        
        print(place,'완료')

    result_cong = {'live':cong_live,'road':cong_road,'wtr':cong_wtr,'prk':cong_prk,'fcst24wtr':cong_fcst24wtr,'sub':cong_sub}
    return result_cong


mergeDf = PythonOperator(task_id='Create.mergedDf',
                    python_callable=merge_data,
                    dag=test_dag,
                    trigger_rule='all_success')


def insert_data_live(**context):
    result = context['task_instance'].xcom_pull(task_ids='Create.mergedDf')
    print(result['live'])
    
    cong_df = pd.DataFrame(result['live'],columns=['AREA_NM', 'AREA_CONGEST_LVL', 'AREA_CONGEST_MSG', 'AREA_PPLTN_MIN', 'AREA_PPLTN_MAX', 'PPLTN_TIME'])
    print(cong_df)
    hh = HiveCliHook(hive_cli_conn_id='hive_cli_connect')
    hh.load_df(df=cong_df,table='cong_live',
               field_dict={
                            'AREA_NM' : 'STRING',
                            'AREA_CONGEST_LVL' :'STRING',
                            'AREA_CONGEST_MSG' :'STRING',
                            'AREA_PPLTN_MIN' :'INT',
                            'AREA_PPLTN_MAX' :'INT',
                            'PPLTN_TIME' :'TIMESTAMP'
                        })

# def insert_data_prk(*op_args,**context):
#     result = context['task_instance'].xcom_pull(task_ids='Create.mergedDf')
#     cong_df = pd.DataFrame(result['prk'],columns=['AREA_NM', 'PRK_NM', 'PRK_CD', 'CPCTY', 'CUR_PRK_CNT', 'CUR_PRK_TIME', 'CUR_PRK_YN', 'PAY_YN', 'RATES', 'TIME_RATES', 'ADD_RATES', 'ADD_TIME_RATES', 'ADDRESS', 'ROAD_ADDR', 'LNG', 'LAT'])
#     hh = HiveCliHook()
#     hh.load_df(df=cong_df,table='cong_prk',
#                field_dict={
#                             'AREA_NM' : 'STRING',
#                             'PRK_NM' :'STRING',
#                             'PRK_CD': 'INT',
#                             'CPCTY' : 'INT',
#                             'CUR_PRK_CNT' : 'INT',
#                             'CUR_PRK_TIME' : 'TIMESTAMP',
#                             'CUR_PRK_YN'  : 'STRING',
#                             'PAY_YN' : 'STRING',
#                             'RATES'  : 'INT',
#                             'TIME_RATES' : 'INT',
#                             'ADD_RATES' : 'INT',
#                             'ADD_TIME_RATES': 'INT',
#                             'ADDRESS' : 'STRING',
#                             'ROAD_ADDR' : 'STRING',
#                             'LNG' : 'DOUBLE',
#                             'LAT' : 'DOUBLE'
#                         })
 
#데이터 전송
i1 = PythonOperator(task_id='Insert.live',
                    python_callable=insert_data_live,
                    dag=test_dag)

# i2 = PythonOperator(task_id='Insert.road',
#                     python_callable=insert_data,
#                     op_args={'df':'road','table':'cong_road'},
#                     dag=test_dag)


# i3 = PythonOperator(task_id='Insert.weather',
#                     python_callable=insert_data,
#                     op_args={'df':'wtr','table':'cong_wtr'},
#                     dag=test_dag)


# i4 = PythonOperator(task_id='Insert.parking',
#                     python_callable=insert_data,
#                     op_args={'df':'prk','table':'cong_prk'},
#                     dag=test_dag)

# i5 = PythonOperator(task_id='Insert.fcst24wtr',
#                     python_callable=insert_data,
#                     op_args={'df':'fcst24wtr','table':'cong_fcst24wtr'},
#                     dag=test_dag)

# i6 = PythonOperator(task_id='Insert.subway',
#                     python_callable=insert_data,
#                     op_args={'df':'sub','table':'cong_sub'},
#                     dag=test_dag)

# hql= f'''
#     INSERT INTO cong_live {cong_live};
#     '''

# h1 = HiveOperator(
#     task_id='HiveOperator_test',
#     hql=hql,
#     hive_cli_conn_id='hive_cli_connect',
#     run_as_owner=True,
#     dag=test_dag,
# )

# init_tb >> [t1,t2,t3,t4,t5] >> check_data  >> mergeDf >> [i1,i2,i3,i4,i5,i6]

init_tb >> get_data >> check_data >> mergeDf >> i1






