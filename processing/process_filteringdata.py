import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, to_date, col, array, when
from pyspark.sql.types import IntegerType, ArrayType, StringType
import sys
import re

# 모델 분류를 위한 product_dict 정의
iPhone14_dict = {
    'iPhone14':re.compile(r'(?i)(?:iPhone|아이폰)\s?14(?!(?:\s?(?:Pro|Plus|Pro\s?Max|프로|플러스|프로\s?맥스)))'),
    'iPhone14Plus':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Plus|플러스)'),
    'iPhone14Pro':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Pro|프로)(?!(?:\s?(?:Max|맥스)))'),
    'iPhone14ProMax':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Pro|프로)\s?(?:Max|맥스)')                   
}

iPhone13_dict = {
    'iPhone13Mini': re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Mini|미니)'),
    'iPhone13': re.compile(r'(?i)(?:iPhone|아이폰)\s?13(?!(?:\s?(?:Mini|미니|Pro|Plus|프로|플러스|Pro\s?Max|프로\s?맥스)))'),
    'iPhone13Pro': re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Pro|프로)(?!(?:\s?(?:Max|맥스)))'),
    'iPhone13ProMax': re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Pro|프로)\s?(?:Max|맥스)')                   
}

product_list = {
    'iPhone14':iPhone14_dict,
    'iPhone13':iPhone13_dict
}


# model 분류를 위한 spark.udf작성
def classify_model(title):
    for model in model_dict:
        match = model_dict[model].search(title)
        if match:
            return model
    return 'None'
classify_model_udf = udf(classify_model, StringType())

# storage 분류를 위한 spark.udf작성
def classify_storage_feature(feature_list, title, context):
    pattern = re.compile(r'(?<!\d)(64|128|256|512|1)(?!\d)', re.IGNORECASE)
    
    title = title if title is not None else ""
    context = context if context is not None else ""

    # title에서 storage 추출
    match = pattern.search(title)
    if match:
        if match.group(1) == '1':
            feature_list.append("1024GB")
        else:
            feature_list.append(match.group(1)+"GB")
        return feature_list

    # title에서 추출 못할 시 context에서 storage 추출
    match = pattern.search(context)
    if match:
        if match.group(1) == '1':
            feature_list.append("1024GB")
        else:
            feature_list.append(match.group(1)+"GB")
        return feature_list
    return feature_list
classify_storage_feature_udf = udf(classify_storage_feature, ArrayType(StringType()))

# 제품의 일반적인 특징을 뽑아내기 위한 spark.udf작성
def extract_general_feature(feature_list, context):
    # context 없는 경우 feature 추출하지 않고 바로 return
    if context is None:
        return feature_list
    
    # 제품이 갖고있는 특징들이 feature_list에 삽입되고 return됨
    pattern = re.compile(r'.*미개봉.*')
    if pattern.search(context):
        feature_list.append('미개봉')
        return feature_list

    # 일반적인 특징을 뽑아내기 위해 정규식 구성에 사용될 단어 및 형태소
    checking_feature_list = ['기스', '깨짐', '잔상', '흠집', '파손', '찍힘']
    checking_morpheme_list = ['있', '존재', '정도']

    for checking_feature in checking_feature_list:
        for checking_morpheme in checking_morpheme_list:
            pattern = re.compile(fr".*{checking_feature}.{{0,10}}{checking_morpheme}.*")
            match = pattern.search(context)
            
            if match:
                feature_list.append(checking_feature)
                break
                    
    checking_feature_list2 = ['부품용']  
    for checking_feature in checking_feature_list2:
        if checking_feature in context:
            feature_list.append(checking_feature)
                
    return feature_list
extract_general_feature_udf = udf(extract_general_feature, ArrayType(StringType()))

# 애플 제품 한정, 애플케어플러스를 추출하기 위한 spark.udf작성
def extract_applecare_feature(feature_list, context):
    # context 없는 경우 feature 추출하지 않고 바로 return
    if context is None:
        return feature_list
        
    pattern = re.compile(r"(케어|캐어|애케|애캐|애플케어|애플캐어|애케플|애캐플).{0,15}(포함|까지|있|적용)")
    match = pattern.search(context)
    if match:
        feature_list.append('애플케어플러스')
        return feature_list
    return feature_list
extract_applecare_feature_udf = udf(extract_applecare_feature, ArrayType(StringType()))

def extract_battery(context):
    # battery 값 확인할 수 없다는 의미로 -1
    if context is None:
        return -1
        
    pattern = re.compile(r'.*미개봉.*')
    if pattern.search(context):
        return 100
        
    pattern = re.compile(r".*배터리.{0,7}(\d{2,3}).{0,3}\s*(퍼센트|프로|%|퍼)")
    match = pattern.search(context)
    if match:
        efficiency = match.group(1)
        
        if efficiency == '00':
            efficiency = '100'
        return int(efficiency)
        
    pattern = re.compile(r".*배터리.{0,7}(\d{2,3}).*")
    match = pattern.search(context)
    if match:
        efficiency = match.group(1)
        
        if efficiency == '00':
            efficiency = '100'
        return int(efficiency)
    return -1
extract_battery_udf = udf(extract_battery, IntegerType())

def feature_list_to_string(feature_list):
    return str(feature_list)
feature_list_to_string_udf = udf(feature_list_to_string, StringType())


def process_filteringdata(df:pyspark.sql.dataframe.DataFrame, product_name):
    spark = SparkSession.builder.appName("processing").getOrCreate()

    df = df.withColumn("feature_list", array())

    global model_dict 
    model_dict = product_list[product_name]

    df = df.withColumn('model', classify_model_udf(df.title))
    df = df.withColumn('feature_list', classify_storage_feature_udf(df.feature_list, df.title, df.context))
    df = df.withColumn('feature_list', extract_general_feature_udf(df.feature_list, df.context))
    df = df.withColumn('feature_list', extract_applecare_feature_udf(df.feature_list, df.context))
    df = df.withColumn('battery', extract_battery_udf(df.context))

    # 모델 분류 안되는 행 삭제
    df = df.filter(col("model") != "None")
    # location 채워 넣기
    df = df.withColumn("location", when((df.location.isNull()) | (df.location == ""), "-").otherwise(df.location))
    # feature_list를 문자열로 변환
    df = df.withColumn('feature_list', feature_list_to_string_udf(df.feature_list))


    df = df.select(['model', 'feature_list', 'battery', 'upload_date', 'price', 'status', 'location', 'img_url'])
    return df
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python3 ~.py ID PW")
        sys.exit(1)
   
    spark = SparkSession.builder \
    .appName("spark-sql") \
    .config("spark.driver.extraClassPath", "~/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    .getOrCreate()

    ip = 'ls-0417629e59c83e2cfae4e2aac001b7eee2799e0e.cxiwwsmmq2ua.ap-northeast-2.rds.amazonaws.com'
    port = '3306'
    user = sys.argv[1]
    passwd = sys.argv[2]
    db = 'joonggoinfo'

    sql = "SELECT * FROM Post_iPhone14"
    df1 = spark.read.format('jdbc') \
                    .option("url", f"jdbc:mysql://{ip}:{port}/{db}")\
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("user", user) \
                    .option("password", passwd) \
                    .option("query", sql) \
                    .load()

    df2 = process_filteringdata(df1, 'iPhone14')
    df2.show()



