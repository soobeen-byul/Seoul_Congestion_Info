# Seoul_Congestion_Info
서울시 실시간 혼잡도 및 도시환경 데이터 파이프라인 생성

## 🔎 프로젝트 개요 
- 서울시 실시간 도시 데이터 api로부터 데이터를 전송받아 hive로 전송
- DATA 원천 : [서울시 열린데이터 광장](https://data.seoul.go.kr/dataList/OA-21285/A/1/datasetView.do)
- 데이터 분류

  ```
  - 실시간 인구 현황
  - 도로 소통 현황
  - 주차장 현황
  - 지하철 실시간 도착 현황
  - 날씨 현황
  - 24시간 예보
## 📊 Ariflow Pipeline

- **Init.table** : 테이블 초기화
- **Get.api_data** : api로부터 50개 장소에 대한 실시간 데이터 받아오기
- **Merge.place_data** : 장소 데이터를 데이터 분류별로 통합
- **Update.raw_** : 병합된 데이터를 hive로 전송하여 raw table 생성
- **Create.partition_table** : 각각의 raw table에서 장소를 기준으로 partitioning

![image](https://github.com/soobeen-byul/Seoul_Congestion_Info/assets/95599133/87c20d6c-0a15-43c1-9a5c-42646141a344)


## 🖥 기술 스택

<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=Apache%20Hadoop&logoColor=white"> <img src="https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=Apache%20Hive&logoColor=white">
