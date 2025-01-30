from datetime import datetime

date_time_str1 = '2021-07-07 01:55:19'
date_time_str2 = '2021-07-17 01:55:19'
date_time_obj1 = datetime.strptime(date_time_str1, '%Y-%m-%d %H:%M:%S')
date_time_obj2 = datetime.strptime(date_time_str2, '%Y-%m-%d %H:%M:%S')
print(date_time_obj1<date_time_obj2)
