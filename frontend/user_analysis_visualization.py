# 参考文献：https://tianchi.aliyun.com/notebook/464175?spm=a2c22.12281897.0.0.a06623b7vJhETF
import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts
from streamlit_echarts import st_pyecharts
from pyecharts.charts import Pie
from pyecharts import options as opts
import time


def plot_pv_uv(pv_daily, uv_daily, title1, title2, unique_id):
    # 获取第一列和第二列
    pv_x_col = pv_daily.iloc[:, 0].astype(str)  # 第一列作为字符串
    pv_y_col = pv_daily.iloc[:, 1]  # 第二列
    uv_x_col = uv_daily.iloc[:, 0].astype(str)  # 第一列作为字符串
    uv_y_col = uv_daily.iloc[:, 1]  # 第二列
    
    # 计算 pv_daily 的 y 轴范围并扩展 10%
    pv_min = pv_y_col.min()
    pv_max = pv_y_col.max()
    pv_range_min = pv_min * 0.9
    pv_range_max = pv_max * 1.1
    
    # 准备 pv_daily 的 ECharts 数据
    pv_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": [title1]
        },
        "xAxis": {
            "type": "category",
            "data": pv_x_col.tolist(),  # 直接使用字符串列表
            "name": "X轴"
        },
        "yAxis": {
            "type": "value",
            "name": "Count",
            "min": int(pv_range_min - 1),
            "max": int(pv_range_max + 1)
        },
        "series": [
            {
                "name": title1,
                "type": "line",
                "data": pv_y_col.tolist(),
                "itemStyle": {
                    "color": "blue"
                }
            }
        ]
    }

    # 渲染 pv_daily 图表
    st_echarts(pv_option, height="500px", key=f"pv_echarts_{unique_id}")

    # 计算 uv_daily 的 y 轴范围并扩展 10%
    uv_min = uv_y_col.min()
    uv_max = uv_y_col.max()
    uv_range_min = uv_min * 0.9
    uv_range_max = uv_max * 1.1
    
    # 准备 uv_daily 的 ECharts 数据
    uv_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": [title2]
        },
        "xAxis": {
            "type": "category",
            "data": uv_x_col.tolist(),  # 直接使用字符串列表
            "name": "X轴"
        },
        "yAxis": {
            "type": "value",
            "name": "Count",
            "min": int(uv_range_min - 1),
            "max": int(uv_range_max + 1)
        },
        "series": [
            {
                "name": title2,
                "type": "line",
                "data": uv_y_col.tolist(),
                "itemStyle": {
                    "color": "green"
                }
            }
        ]
    }

    # 渲染 uv_daily 图表
    st_echarts(uv_option, height="500px", key=f"uv_echarts_{unique_id}")

def main():
    # 加载数据
    data_user = pd.read_csv('../../Downloads/datasets/spark_class/user_action.csv')
    
    # 分割天(date)和小时(hour)
    data_user['date'] = data_user['time'].map(lambda x: x.split(' ')[0])
    data_user['hour'] = data_user['time'].map(lambda x: x.split(' ')[1])
    
    # 类型转换
    data_user['user_id'] = data_user['user_id'].astype('object')
    data_user['item_id'] = data_user['item_id'].astype('object')
    data_user['item_category'] = data_user['item_category'].astype('object')
    data_user['date'] = pd.to_datetime(data_user['date'])
    data_user['hour'] = data_user['hour'].astype('int64')
    
    runtime_res= {}

    # 计算PV
    start_time = time.time()
    pv_daily = data_user.groupby('date')['user_id'].count()
    ed = time.time()
    pv_daily = pv_daily.reset_index() 
    pv_daily = pv_daily.rename(columns={'user_id':'pv_daily'})
    runtime_res['pv_daily'] = ed - start_time
    
    # 计算UV
    start_time = time.time()
    uv_daily = data_user.groupby('date')['user_id'].apply(lambda x: len(x.unique()))
    ed = time.time()
    uv_daily = uv_daily.reset_index()
    uv_daily = uv_daily.rename(columns={'user_id':'uv_daily'})
    runtime_res['uv_daily'] = ed - start_time

    # 绘制PV和UV，传入唯一的 ID
    plot_pv_uv(pv_daily, uv_daily, title1="PV Daily", title2="UV Daily", unique_id="daily")

    # 计算每小时的PV
    start_time = time.time()
    pv_hour = data_user.groupby('hour')['user_id'].count()
    ed = time.time()
    pv_hour = pv_hour.reset_index()
    pv_hour = pv_hour.rename(columns={'user_id':'pv_hour'})
    runtime_res['pv_hour'] = ed - start_time
    
    # 计算每小时UV
    start_time = time.time()
    uv_hour = data_user.groupby('hour')['user_id'].apply(lambda x: len(x.unique()))
    ed = time.time()
    uv_hour = uv_hour.reset_index()
    uv_hour = uv_hour.rename(columns={'user_id':'uv_hour'})
    runtime_res['uv_hour'] = ed - start_time

    # 可视化
    plot_pv_uv(pv_hour, uv_hour, "PV Hour", "UV Hour", unique_id="hourly")
    
    # 计算12-12的每小时PV
    start_time = time.time()
    data_user_1212 = data_user.loc[data_user['date']=='2014-12-12']
    pv_hour_1212 = data_user_1212.groupby('hour')['user_id'].count().reset_index().rename(columns={'user_id':'1212_pv_hour'})
    uv_hour_1212 = data_user_1212.groupby('hour')['user_id'].apply(lambda x: len(x.unique())).reset_index().rename(columns={'user_id':'1212_uv_hour'})
    ed = time.time()
    runtime_res['pv_uv_hour_1212'] = ed - start_time

    # 可视化12-12每小时的PV和UV
    plot_pv_uv(pv_hour, pv_hour_1212, "PV Hour", "12-12 PV Hour", unique_id="pv_hour_1212")
    plot_pv_uv(uv_hour, uv_hour_1212, "UV Hour", "12-12 UV Hour", unique_id="uv_hour_1212")
    
    # 基于 behavior_type & hour 分组
    start_time = time.time()
    pv_behavior = data_user.groupby(['behavior_type','hour'])['user_id'].count().reset_index()
    pv_behavior = pv_behavior.rename(columns={'user_id':'pv_behavior'})
    runtime_res['pv_behavior'] = ed - start_time

    behavior_plot_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": [f"Behavior {behavior}" for behavior in pv_behavior['behavior_type'].unique()]  # 根据 unique 行为生成图例
        },
        "xAxis": {
            "type": "category",
            "data": list(sorted(set(pv_behavior['hour'].tolist()))),
            "name": "Hour"
        },
        "yAxis": {
            "type": "value",
            "name": "Count"
        },
        "series": []
    }

    # 定义颜色列表
    colors = ['#FF5733', '#33FF57', '#3357FF', '#FFFF33']  # 自定义的颜色列表

    for index, behavior in enumerate(pv_behavior['behavior_type'].unique()):
        filtered_data = pv_behavior[pv_behavior['behavior_type'] == behavior]
        behavior_plot_option["series"].append({
            "name": f"Behavior {behavior}",
            "type": "line",
            "data": filtered_data['pv_behavior'].tolist(),
            "itemStyle": {
                "color": colors[index % len(colors)]  # 使用自定义颜色列表中的颜色
            }
        })

    # 绘制完整的行为折线图
    st_echarts(behavior_plot_option, height="500px", key="pv_behavior_echarts")

    # 复制并调整选项，去掉第一条行为数据
    behavior_plot_option_1 = behavior_plot_option.copy()
    behavior_plot_option_1["series"] = behavior_plot_option_1["series"][1:]
    st_echarts(behavior_plot_option_1, height="500px", key="pv_behavior_echarts_234")
    
    # 浏览 >> 加购/收藏 >> 购买（4）
    data_user_buy = data_user[data_user.behavior_type==4].groupby('user_id')['behavior_type'].count()
    # 将 DataFrame 转换为包含 x 和 y 的形式
    data_user_buy_df = data_user_buy.reset_index()
    data_user_buy_df.columns = ['user_id', 'buy_count']  # 重命名列

    # 设置 ECharts 选项
    buy_plot_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": ["Buy Count"]
        },
        "xAxis": {
            "type": "category",
            "data": data_user_buy_df['user_id'].astype(str).tolist(),  # 确保 user_id 转为字符类型
            "name": "User ID"
        },
        "yAxis": {
            "type": "value",
            "name": "Buy Count"
        },
        "series": [
            {
                "name": "Buy Count",
                "type": "bar",  # 选择条形图类型，可以选择 'line' 或 'scatter' 等
                "data": data_user_buy_df['buy_count'].tolist(),
                "itemStyle": {
                    "color": "blue"  # 可以自定义颜色
                }
            }
        ]
    }

    # 显示图表
    st_echarts(buy_plot_option, height="500px", key="buy_count_echarts")
    # data_user_buy.plot(x='user_id',y='buy_count')
    
    # 计算ARPU
    start_time = time.time()
    data_user['action'] = 1 
    data_user_arpu = data_user.groupby(['date','user_id','behavior_type'])['action'].count()
    data_user_arpu = data_user_arpu.reset_index()
    arpu = data_user_arpu.groupby('date').apply(lambda x: x[x['behavior_type'] == 4]['action'].sum() / len(x['user_id'].unique()) )
    ed = time.time()
    runtime_res['arpu'] = ed - start_time

    # 可视化 ARPU
    arpu_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": ["ARPU"]
        },
        "xAxis": {
            "type": "category",
            "data": arpu.index.strftime('%Y-%m-%d').tolist(),
            "name": "Date"
        },
        "yAxis": {
            "type": "value",
            "name": "ARPU"
        },
        "series": [
            {
                "name": "ARPU",
                "type": "line",
                "data": arpu.tolist(),
                "itemStyle": {
                    "color": "orange"
                }
            }
        ]
    }
    
    # 绘制 ARPU 图表
    st_echarts(arpu_option, height="500px", key="arpu_echarts")

    # 计算每日的所有用户的购买次数
    start_time = time.time()
    data_user_arppu = data_user[data_user['behavior_type']==4].groupby(['date','user_id'])['behavior_type'].count()
    data_user_arppu = data_user_arppu.reset_index().rename(columns={'behavior_type':'buy_count'})

    # 计算ARPPU
    data_user_arppu = data_user_arppu.groupby('date').apply(lambda x:x['buy_count'].sum() / x['user_id'].count())
    ed = time.time()
    runtime_res['data_user_arppu'] = ed - start_time

    arppu_min = data_user_arppu.min()
    arppu_max = data_user_arppu.max()

    # 计算扩展的范围
    arppu_range_min = arppu_min * 0.9  # 最小值扩展10%
    arppu_range_max = arppu_max * 1.1  # 最大值扩展10%

    arppu_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": ["ARPPU"]
        },
        "xAxis": {
            "type": "category",
            "data": data_user_arppu.index.strftime('%Y-%m-%d').tolist(),
            "name": "Date"
        },
        "yAxis": {
            "type": "value",
            "name": "ARPPU",
            "min": arppu_range_min,  # 设置 y 轴最小值
            "max": int(arppu_range_max),  # 设置 y 轴最大值
        },
        "series": [
            {
                "name": "ARPPU",
                "type": "line",
                "data": data_user_arppu.tolist(),
                "itemStyle": {
                    "color": "orange"
                }
            }
        ]
    }

    # 绘制ARPPU
    st_echarts(arppu_option, height="500px", key="arppu_echarts")

    # 计算用户购买频次和复购率
    data_user_pay = data_user[data_user.behavior_type==4]
    data_user_pay = data_user_pay.groupby('user_id')['date'].apply(lambda x: len(x.unique()))

    repeat_buy_ratio = data_user_pay[data_user_pay > 1].count() / data_user_pay.count()

    st.write(f"复购率: {repeat_buy_ratio}")

    start_time = time.time()
    data_user['action'] = 1 # 对每一行的行为记为1次，通过对行为次数的相加，从而计算频次
    data_user_buy = data_user[data_user.behavior_type == 4]
    data_user_buy = data_user_buy.groupby(['user_id','date'])['action'].count()
    data_user_buy = data_user_buy.reset_index()
    
    # 对日期排序（按照先后顺序），给予pandas的date函数计算前后两次购物相差的天数，dropna去掉了每个用户在数据集周期内第一次购买日期的记录：
    data_user_buy_date_diff = data_user_buy.groupby('user_id').date.apply(lambda x:x.sort_values().diff(1).dropna())
    ed = time.time()
    runtime_res['data_user_buy_date_diff'] = ed - start_time
    print(f"data_user_buy_date_diff = {data_user_buy_date_diff}, time = {ed - start_time} seconds")
    data_user_buy_date_diff = data_user_buy_date_diff.apply(lambda x:x.days)
    repeat_day_diff_counts = data_user_buy_date_diff.value_counts().reset_index()
    repeat_day_diff_counts.columns = ['repeat_day_diff', 'count']

    # 准备 ECharts 图表数据
    data_user_buy_date_diff_option = {
        "tooltip": {
            "trigger": "axis"
        },
        "legend": {
            "data": ["Count"]
        },
        "xAxis": {
            "type": "category",
            "data": repeat_day_diff_counts['repeat_day_diff'].astype(str).tolist(),  # 将重复日期差转为字符串
            "name": "Repeat Day Difference"
        },
        "yAxis": {
            "type": "value",
            "name": "Count"
        },
        "series": [
            {
                "name": "Count",
                "type": "line",
                "data": repeat_day_diff_counts['count'].tolist(),
                "itemStyle": {
                    "color": "blue"
                }
            }
        ]
    }

    # 显示 ECharts 图表
    st_echarts(data_user_buy_date_diff_option, height="500px")


def save_result(runtime_res):
    import json
    json_file = './result.json'
    with open(json_file, 'w') as f:
        json.dump(runtime_res, f, indent=4)
    print(f"数据已写入到 {json_file}")
    

if __name__ == "__main__":
    st.set_page_config(
        page_title="电商平台用户购物行为数据可视化", page_icon=":chart_with_upwards_trend:"
    )
    main()
