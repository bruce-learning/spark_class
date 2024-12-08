from pyspark.sql import SparkSession
from datetime import datetime
import time
import os

pv_daily_out_path = "/homework/out/pv_daily"
uv_daily_out_path = "/homework/out/uv_daily"
pv_hourly_out_path = "/homework/out/pv_hourly"
uv_hourly_out_path = "/homework/out/uv_hourly"
pv_uv_hourly_1212_out_path = "/homework/out/pv_uv_hourly_1212"
pv_behavior_out_path = "/homework/out/pv_behavior"
arpu_out_path = "/homework/out/arpu"
arppu_out_path = "/homework/out/arppu"
data_user_buy_date_diff_out_path = "/homework/out/data_user_buy_date_diff"

log_file_path = "/homework/out/time.log"

spark = SparkSession.builder.appName("Analyse").getOrCreate()

log_rdd = spark.sparkContext.getOrCreate().parallelize([])

def log_execution_time(description, start_time, end_time):
    execution_time = end_time - start_time
    log_message = f"{description}: {execution_time:.3f} seconds\n"
    global log_rdd
    log_rdd = log_rdd.union(spark.sparkContext.getOrCreate().parallelize([log_message]))

data_user = spark.sparkContext.textFile("hdfs://node1:9000/homework/user_action.csv")

header = data_user.first()
data_user = data_user.filter(lambda row: row != header)

def parse_line(line):
    fields = line.split(',')
    return {
        "user_id": fields[0],
        "item_id": fields[1],
        "behavior_type": fields[2],
        "item_category": fields[3],
        "time": fields[4]
    }

data_user = data_user.map(parse_line)

# print(header)
# print(data_user.take(20))

user_count = data_user.map(lambda row: row["user_id"]).distinct().count()
item_count = data_user.map(lambda row: row["item_id"]).distinct().count()
category_count = data_user.map(lambda row: row["item_category"]).distinct().count()
print(f"Data count: {data_user.count()}")
print(f"User count: {user_count}")
print(f"Item count: {item_count}")
print(f"Category count: {category_count}")

null_counts = {}
for i, col in enumerate(header.split(',')):
    null_counts[col] = data_user.filter(lambda row: len(row) <= i or row[col] is None or row[col].strip() == "").count()
for col, count in null_counts.items():
    print(f"Column '{col}' has {count} null values.")

data_user = data_user.map(
    lambda row: {**row, "date": row["time"].split(" ")[0], "hour": int(row["time"].split(" ")[1])}
)

# print(data_user.take(20))

# ---------- 流量分析 ----------
# ----- 基于天级别的访问流量分析 -----
start_time = time.time()
pv_daily = (
    data_user.map(lambda row: (row["date"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"date": row[0], "pv_daily": row[1]})
        .collect()
)
end_time = time.time()
log_execution_time("Daily PV", start_time, end_time)
pv_daily_df = spark.createDataFrame(pv_daily)
print("Daily PV:")
pv_daily_df.show()
pv_daily_df.coalesce(1).write.mode("overwrite").json(pv_daily_out_path)

start_time = time.time()
uv_daily = (
    data_user.map(lambda row: (row["date"], row["user_id"]))
        .distinct()
        .map(lambda row: (row[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"date": row[0], "uv_daily": row[1]})
        .collect()
)
end_time = time.time()
log_execution_time("Daily UV", start_time, end_time)
uv_daily_df = spark.createDataFrame(uv_daily)
print("Daily UV:")
uv_daily_df.show()
uv_daily_df.coalesce(1).write.mode("overwrite").json(uv_daily_out_path)

# ----- 基于小时级别的访问流量分析 -----
start_time = time.time()
pv_hourly = (
    data_user.map(lambda row: (row["hour"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"hour": row[0], "pv_hourly": row[1]})
        .collect()
)
end_time = time.time()
log_execution_time("Hourly PV", start_time, end_time)
pv_hourly_df = spark.createDataFrame(pv_hourly)
print("Hourly PV:")
pv_hourly_df.show()
pv_hourly_df.coalesce(1).write.mode("overwrite").json(pv_hourly_out_path)

start_time = time.time()
uv_hourly = (
    data_user.map(lambda row: (row["hour"], row["user_id"]))
        .distinct()
        .map(lambda row: (row[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"hour": row[0], "uv_hourly": row[1]})
        .collect()
)
end_time = time.time()
log_execution_time("Hourly UV", start_time, end_time)
uv_hourly_df = spark.createDataFrame(uv_hourly)
print("Hourly UV:")
uv_hourly_df.show()
uv_hourly_df.coalesce(1).write.mode("overwrite").json(uv_hourly_out_path)


start_time = time.time()
data_user_1212 = data_user.filter(lambda row: row["date"] == "2014-12-12")
pv_hourly_1212 = (
    data_user_1212.map(lambda row: (row["hour"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"hour": row[0], "pv_hourly": row[1]})
        .collect()
)
uv_hourly_1212 = (
    data_user_1212.map(lambda row: (row["hour"], row["user_id"]))
        .distinct()
        .map(lambda row: (row[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"hour": row[0], "uv_hourly": row[1]})
        .collect()
)
pv_uv_hourly_1212 = (
    spark.createDataFrame(pv_hourly_1212)
        .join(spark.createDataFrame(uv_hourly_1212), on="hour")
        .collect()
)
end_time = time.time()
pv_uv_hourly_1212_df = spark.createDataFrame(pv_uv_hourly_1212)
print("Hourly PV & UV on 2014-12-12:")
log_execution_time("Hourly PV & UV on 2014-12-12", start_time, end_time)
pv_uv_hourly_1212_df.show()
pv_uv_hourly_1212_df.coalesce(1).write.mode("overwrite").json(pv_uv_hourly_1212_out_path)

# ----- 不同用户行为流量分析 -----
start_time = time.time()
pv_behavior = (
    data_user.map(lambda row: ((row["behavior_type"], row["hour"]), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"behavior_type": row[0][0], "hour": row[0][1], "pv_behavior": row[1]})
        .collect()
)
end_time = time.time()
log_execution_time("Behavior PV", start_time, end_time)
pv_behavior_df = spark.createDataFrame(pv_behavior)
print("Behavior PV:")
pv_behavior_df.show()
pv_behavior_df.coalesce(1).write.mode("overwrite").json(pv_behavior_out_path)

# ---------- 转化率分析 ----------
behavior_type = (
    data_user.map(lambda row: (row["behavior_type"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: row[1])
        .collect()
)
click_num, fav_num, add_num, pay_num = behavior_type[0], behavior_type[1], behavior_type[2], behavior_type[3]
fav_add_num = fav_num + add_num
print(f"加购/收藏转化率: {100 * fav_add_num / click_num}")
print(f"点击/购买转化率: {100 * pay_num / click_num}")
print(f"加购/收藏 到 购买转化率: {100 * pay_num / fav_add_num}")

behavior_type = (
    data_user.filter(lambda row: row["date"] == "2014-12-12")
        .map(lambda row: (row["behavior_type"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: row[1])
        .collect()
)
click_num, fav_num, add_num, pay_num = behavior_type[0], behavior_type[1], behavior_type[2], behavior_type[3]
fav_add_num = fav_num + add_num
print(f"双十二 加购/收藏转化率: {100 * fav_add_num / click_num}")
print(f"双十二 点击/购买转化率: {100 * pay_num / click_num}")
print(f"双十二 加购/收藏 到 购买转化率: {100 * pay_num / fav_add_num}")

# ---------- 用户价值分析 ----------
# ----- 用户购买频次分析 -----
data_user_buy = (
    data_user.filter(lambda row: row["behavior_type"] == "4")
        .map(lambda row: (int(row["user_id"]), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"user_id": row[0], "buy_count": row[1]})
        .collect()
)
data_user_buy_df = spark.createDataFrame(data_user_buy)
print("User Buy Count:")
data_user_buy_df.show()

# ----- ARPU 分析 -----
start_time = time.time()
data_user_arpu = (
    data_user.map(lambda row: ((row["date"], int(row["user_id"]), row["behavior_type"]), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"date": row[0][0], "user_id": row[0][1], "behavior_type": row[0][2], "action": row[1]})
)
# data_user_arpu_df = spark.createDataFrame(data_user_arpu.collect())
# print("User ARPU:")
# data_user_arpu_df.show()

arpu_buy_count = (
    data_user_arpu.filter(lambda row: row["behavior_type"] == "4")
        .map(lambda row: (row["date"], row["action"]))
        .reduceByKey(lambda a, b: a + b)
)
arpu_unique_user = (
    data_user_arpu.map(lambda row: (row["date"], row["user_id"]))
        .distinct()
        .map(lambda row: (row[0], 1))
        .reduceByKey(lambda a, b: a + b)
)
arpu = (
    arpu_buy_count.join(arpu_unique_user)
        .map(lambda row: {"date": row[0], "arpu": row[1][0] / row[1][1]})
        .sortBy(lambda row: row["date"])
        .collect()
)
end_time = time.time()
log_execution_time("ARPU", start_time, end_time)
arpu_df = spark.createDataFrame(arpu)
print("ARPU:")
arpu_df.show()
arpu_df.coalesce(1).write.mode("overwrite").json(arpu_out_path)

# ----- ARPPU 分析 -----
start_time = time.time()
data_user_arppu = (
    data_user.filter(lambda row: row["behavior_type"] == "4")
        .map(lambda row: ((row["date"], int(row["user_id"])), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"date": row[0][0], "user_id": row[0][1], "buy_count": row[1]})
)
# data_user_arppu_df = spark.createDataFrame(data_user_arppu.collect())
# print("User ARPPU:")
# data_user_arppu_df.show()

arppu_buy_count = (
    data_user_arppu.map(lambda row: (row["date"], row["buy_count"]))
        .reduceByKey(lambda a, b: a + b)
)

arppu_user = (
    data_user_arppu.map(lambda row: (row["date"], 1))
        .reduceByKey(lambda a, b: a + b)
)

arppu = (
    arppu_buy_count.join(arppu_user)
        .map(lambda row: {"date": row[0], "arppu": row[1][0] / row[1][1]})
        .sortBy(lambda row: row["date"])
        .collect()
)
end_time = time.time()
log_execution_time("ARPPU", start_time, end_time)
arppu_df = spark.createDataFrame(arppu)
print("ARPPU:")
arppu_df.show()
arppu_df.coalesce(1).write.mode("overwrite").json(arppu_out_path)

# ----- 复购情况分析 -----
data_user_pay = (
    data_user.filter(lambda row: row["behavior_type"] == "4")
        .map(lambda row: (int(row["user_id"]), row["date"]))
        .distinct()
        .map(lambda row: (row[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"user_id": row[0], "pay_count": row[1]})
)
data_user_pay_df = spark.createDataFrame(data_user_pay.collect())
print("User Pay Count:")
data_user_pay_df.show()

repeat_buy_radio = (
    data_user_pay.filter(lambda row: row["pay_count"] > 1)
        .map(lambda row: row["user_id"])
        .count() / data_user_pay.count()
)
print(f"复购率: {repeat_buy_radio}")

# ----- 复购周期分析 -----
start_time = time.time()
data_user_buy = (
    data_user.filter(lambda row: row["behavior_type"] == "4")
        .map(lambda row: ((int(row["user_id"]), row["date"]), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .map(lambda row: {"user_id": row[0][0], "date": row[0][1], "buy_count": row[1]})
)
# data_user_buy_df = spark.createDataFrame(data_user_buy.collect())
# print("User Buy Count:")
# data_user_buy_df.show()

def calculate_date_diffs(sorted_dates):
    diffs = [
        (datetime.strptime(sorted_dates[i + 1], "%Y-%m-%d") - datetime.strptime(sorted_dates[i], "%Y-%m-%d")).days
        for i in range(len(sorted_dates) - 1)
    ]
    return diffs

data_user_buy_date_diff = (
    data_user_buy.map(lambda row: (row["user_id"], row["date"]))
        .groupByKey()
        .mapValues(lambda dates: sorted(dates))
        .mapValues(calculate_date_diffs)
        .flatMapValues(lambda date_diffs: date_diffs)
        .sortByKey()
        .map(lambda row: {"user_id": row[0], "date_diffs": row[1]})
)
end_time = time.time()
log_execution_time("User Buy Date Diff", start_time, end_time)
data_user_buy_date_diff_df = spark.createDataFrame(data_user_buy_date_diff.collect())
print("User Buy Date Diff:")
data_user_buy_date_diff_df.show()
data_user_buy_date_diff_df.coalesce(1).write.mode("overwrite").json(data_user_buy_date_diff_out_path)

log_rdd.coalesce(1).saveAsTextFile(log_file_path)