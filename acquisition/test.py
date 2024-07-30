import pymysql

db = pymysql.connect(host='ls-0417629e59c83e2cfae4e2aac001b7eee2799e0e.cxiwwsmmq2ua.ap-northeast-2.rds.amazonaws.com',
                      user='admin', passwd='12312312', db='joonggoinfo')
cursor = db.cursor()
cursor.execute('SELECT * FROM Urls')
data = cursor.fetchall()
print(data)
# cursor.execute('INSERT INTO Urls (num, site_name, keyword) VALUES (1234123, "joongnaSite", "Iphone14")')
