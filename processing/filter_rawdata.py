from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, udf, StringType
import sys

# 몇 년전, 몇 달전, 몇 주전 3가지 경우 몇 일 거래인지 정확한 판독불가
def filtering_date(upload_date):
    if upload_date is None:
        return None
    
    if '00' in upload_date:
        # 2023년 거래인 것은 확실, 월 일 부정확한 데이터, 0023년은 2023년 1년치 계산시에만 사용
        if '00.00' in upload_date:
            return '0023.01.01'
        # 년 월은 분명하지만 일은 부정확한 데이터 분류, 102X년 0X월 01일은 202X년 0X월 데이터 분석시에만 사용
        return upload_date.replace('00','01').replace('20','10')
    return upload_date
filtering_date_udf = udf(filtering_date, StringType())


def filter_rawdata(rawdata_file_path, product_name_string, product_name_num):

    # raw data 품목에 따라서 file_path설정
    # 예시: "/home/boui/Desktop/usedGoods_data/rawdata/iphone14_joongnaCafe.csv"
    rawdata_file_path_bunjang = rawdata_file_path + product_name_string + product_name_num + '_bunjang.csv'
    rawdata_file_path_joongnaCafe = rawdata_file_path + product_name_string + product_name_num + "_joongnaCafe.csv"
    rawdata_file_path_joongnaSite = rawdata_file_path + product_name_string + product_name_num + "_joongnaSite.csv"
    rawdata_file_path_daangn = rawdata_file_path + product_name_string + "_daangn.csv"

    df_bunjang = spark.read.csv(rawdata_file_path_bunjang, header = True, multiLine=True, quote='"', escape='"')
    df_joongnaCafe = spark.read.csv(rawdata_file_path_joongnaCafe, header = True, multiLine=True, quote='"', escape='"')
    df_joongnaSite = spark.read.csv(rawdata_file_path_joongnaSite, header = True, multiLine=True, quote='"', escape='"')
    df_daangn = spark.read.csv(rawdata_file_path_daangn , header = True, multiLine=True, quote='"', escape='"')

    #bunjang만 condition column이 추가로 있어서 삭제
    df_bunjang = df_bunjang.drop(df_bunjang.condition)

    #3사 rawdata 통합
    df = df_bunjang.union(df_joongnaCafe).union(df_joongnaSite).union(df_daangn)
    print("raw_rows_count:", df.count())

    # 날짜 값 변환 완료
    df = df.withColumn('upload_date', filtering_date_udf(df.upload_date))

    # data type 변환부분
    df = df.withColumn('price', df.price.cast("int")) \
        .withColumn('status', df.status.cast('int')) \
        .withColumn('upload_date', to_date(df.upload_date, 'yyyy.MM.dd'))

    # 실제 filtering하는 영역--------------------------------------------------------#
    df = df.filter(df.upload_date.isNotNull())
    df = df.filter(~(col('context').rlike('업체|매입|최저가|최고가')))
    
    # cast글 및 과대 가격글 제거, case게시글이 많아서 case 판매 가격이 이상치로 분류가 안됨 가격 수동설정 필요
    df = df.filter((col("price") >= 200000) & (col("price") <= 3000000))

    # 중복 게시글 제거
    df = df.dropDuplicates(["title", "context"])

    print("filtered_rows_count:", df.count())
    # filtering data csv 생성
    output_file_path = rawdata_file_path + product_name_string + product_name_num + "_filtering.csv"
    df = df.toPandas()
    df.to_csv(output_file_path, index = False, header = True, encoding = 'utf-8-sig') # Pandas로 쓰기
    # df.write.csv(output_file_path, header=True, quote='"', escape='"') # Spark로 쓰기
    print("Created,", output_file_path)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("usage: python3 filter_rawdata.py <rawdata_file_path> <product_name_string> <product_name_num>")
        print("example: python3 filter_rawdata.py hdfs://master01:9000/user/bigdata/ iphone 14")
        sys.exit(1)
    spark = SparkSession.builder.appName("FilteringApp").getOrCreate()
    rawdata_file_path = sys.argv[1]
    product_name_string = sys.argv[2]
    product_name_num = sys.argv[3]
    filter_rawdata(rawdata_file_path, product_name_string, product_name_num)
