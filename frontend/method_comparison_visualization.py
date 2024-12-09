import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts
from streamlit_echarts import st_pyecharts
from pyecharts.charts import Pie
from pyecharts import options as opts


def create_pie_chart(title, data):
    """
    创建饼图的函数
    """
    metric_name = ["arppu", "arpu", "data_user_buy_date_diff", "pv_behavior", "pv_daily", "pv_hourly", "pv_uv_hourly_1212", "uv_daily", "uv_hourly"]
    pie = (
        Pie()
        .add("", [list(z) for z in zip(metric_name, data[1:])])  # 第一项为标签，其余为值
        .set_global_opts(legend_opts=opts.LegendOpts(is_show=False)) # 取消图例显示
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
    )

    return pie
  

def four_methods_comparison():
    # 数据源
    data = [
        ["mapreduce", 9.255, 36.273, 8.150, 19.156, 14.193, 14.582, 9.640, 28.300, 20.453],
        ["spark", 5.200, 8.697, 5.058, 5.402, 5.181, 5.047, 9.779, 6.625, 6.342],
        ["flink", 1.3438, 1.5153, 1.1564, 0.9018, 0.7715, 0.9822,  0.9887, 0.7715, 0.9822],
    ]

    # 保留三位小数并四舍五入
    for row in data:
        for i in range(1, len(row)):  # 从 1 开始，跳过第一项（名称）
            row[i] = round(row[i], 3)
    
    # 运行时间具体数据表格
    columns = ["Method", "arppu", "arpu", "data_user_buy_date_diff", "pv_behavior", "pv_daily", "pv_hourly", "pv_uv_hourly_1212", "uv_daily", "uv_hourly"]
    df = pd.DataFrame(data, columns=columns)

    # 显示表格
    st.header("运行时间详情")
    st.table(df)  # 或者使用 st.dataframe(df)
    
    # 三种方法对比
    st.header("三种方法总运行时间以及分任务运行时间对比")
    option = {
        "legend": {},
        "tooltip": {"trigger": "axis", "showContent": False},
        "dataset": {
            "source": [
                ["metric", "arpu", "data_user_buy_date_diff", "pv_behavior", "pv_daily", "pv_hourly", "pv_uv_hourly_1212", "uv_daily", "uv_hourly"],
                *data
            ]
        },
        "xAxis": {
            "type": "category",
            "axisLabel": {
                "rotate": 30,  # 旋转标签以减少重叠
                "interval": 0,  # 显示所有标签
            }
        },
        "yAxis": {"gridIndex": 0},
        "grid": {
            "top": "60%",  # 上边距
            "bottom": "15%",  # 下边距，给标签留出空间
            "left": "10%",  # 左边距
            "right": "10%",  # 右边距
        },
        "series": [
            {
                "type": "line",
                "smooth": True,
                "seriesLayoutBy": "row",
                "emphasis": {"focus": "series"},
            },
            {
                "type": "line",
                "smooth": True,
                "seriesLayoutBy": "row",
                "emphasis": {"focus": "series"},
            },
            {
                "type": "line",
                "smooth": True,
                "seriesLayoutBy": "row",
                "emphasis": {"focus": "series"},
            },
            {
                "type": "pie",
                "id": "pie",
                "radius": "30%",
                "center": ["50%", "25%"],
                "emphasis": {"focus": "data"},
                "label": {"formatter": "{b}: {c} ({d}%)"},
                "encode": {"itemName": "metric", "value": "value", "tooltip": "value"},
            },
        ],
    }

    # 计算每个方法的总和
    total_data = {
        "mapreduce": sum(option["dataset"]["source"][1][1:]),
        "spark": sum(option["dataset"]["source"][2][1:]),
        "flink": sum(option["dataset"]["source"][3][1:]),
    }

    # 更新饼图的数据
    option["series"][3]["data"] = [
        {"value": total_data["mapreduce"], "name": "mapreduce"},
        {"value": total_data["spark"], "name": "spark"},
        {"value": total_data["flink"], "name": "flink"},
    ]

    # 绘制图表
    st_echarts(option, height="500px", key="echarts")

    # 三种方法各自的分任务时间占比
    st.header("三种方法的各自分任务运行时间比例")
    
    # 遍历每种方法，绘制三个饼图
    for method in data:
        method_name = method[0]
        pie_chart = create_pie_chart(method_name, method)
        
        # 渲染图表
        st.subheader(f"{method_name}")
        st_pyecharts(pie_chart)
def main():
    four_methods_comparison()
    

if __name__ == "__main__":
    st.set_page_config(
        page_title="电商平台用户购物行为数据可视化及不同数据计算方法效率分析", page_icon=":chart_with_upwards_trend:"
    )
    main()
