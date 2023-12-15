# -*- coding: utf-8 -*-

# 모듈 import
import pandas as pd
import math
import os
import subprocess
import numpy as np
from tqdm import tqdm
import socket

print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡴⡝⡮⣚⢖⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡠⣖⢵⡹⡜⡮⡺⡜⡵⠅⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⣝⢜⡎⣞⢜⣎⢧⠫⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠳⡱⡝⣜⢕⠎⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢨⢨⢰⠠⡄⡄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⢆⢣⢱⢘⢔⠕⡅⢇⢕⢕⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡐⡜⢜⢌⢎⠜⠔⠕⠕⡱⡑⡜⢌⢆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡪⡸⡨⡢⠃⠀⠀⠀⠀⠑⢜⢸⢨⢒⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢕⠜⡔⡱⡁⠀⠀⠀⠀⢈⢎⠪⡢⠣⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢎⢪⠢⡣⡱⢠⢠⠢⡣⡱⡑⡕⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠢⢣⢱⠸⡘⡔⢕⠕⡜⠌⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠐⠕⢕⢱⠑⠑⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")
print("⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀")

print("     -  안전한 직거래를 위한 직거래 장소 추천 프로그램  -     ")
print("     - ..... 로딩 중입니다 ..... -     ")

# 파일 경로
A_police = "combined_police_data_seoul.csv"
A_security_light = "security_light.csv"
A_emergency_bell = "emergency_bell.csv"
A_cctv = "CCTV_data.csv"

J_bus = "Buslocation.csv"
J_subway = "Subwaylocation.csv"

R_landmarks = "landmarks.csv"
R_stores = "Storelocation.csv"

# CSV 파일 불러오기
A_police_df = pd.read_csv(A_police, encoding="utf-8", low_memory=False)
A_security_light_df = pd.read_csv(A_security_light, encoding="utf-8", low_memory=False)
A_emergency_bell_df = pd.read_csv(A_emergency_bell, encoding="utf-8", low_memory=False)
A_cctv_df = pd.read_csv(A_cctv, encoding="UTF-8-SIG", low_memory=False)

J_bus_df = pd.read_csv(J_bus, encoding="utf-8", low_memory=False)
J_subway_df = pd.read_csv(J_subway, encoding="EUC-KR", low_memory=False)

R_landmarks_df = pd.read_csv(R_landmarks, encoding="utf-8", low_memory=False)
R_stores_df = pd.read_csv(R_stores, encoding="utf-8", low_memory=False)


def find_center(A, B):
    lat1, lon1 = math.radians(A[0]), math.radians(A[1])
    lat2, lon2 = math.radians(B[0]), math.radians(B[1])

    dLon = lon2 - lon1

    Bx = math.cos(lat2) * math.cos(dLon)
    By = math.cos(lat2) * math.sin(dLon)

    lat3 = math.atan2(
        math.sin(lat1) + math.sin(lat2),
        math.sqrt((math.cos(lat1) + Bx) * (math.cos(lat1) + Bx) + By * By),
    )

    lon3 = lon1 + math.atan2(By, math.cos(lat1) + Bx)

    return [math.degrees(lat3), math.degrees(lon3)]


# 사용자로부터 입력 받기
A = list(map(float, input("구매자의 위도와 경도를 입력해 주세요 (쉼표로 구분): ").split(",")))
B = list(map(float, input("판매자의 위도와 경도를 입력해 주세요 (쉼표로 구분): ").split(",")))

# 중심점 찾기
center = find_center(A, B)

center_lat, center_lon = center


# haversine 함수 정의
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def calculate_longitude_diff(lat):
    return 0.009 / math.cos(math.radians(lat))


df_list = [
    A_police_df,
    A_security_light_df,
    A_emergency_bell_df,
    A_cctv_df,
    J_bus_df,
    J_subway_df,
    R_landmarks_df,
    R_stores_df,
]

filtered_list = [
    df[
        (df["lat"].between(center_lat - 0.009, center_lat + 0.009))
        & (
            df["lng"].between(
                center_lon - calculate_longitude_diff(center_lat),
                center_lon + calculate_longitude_diff(center_lat),
            )
        )
    ]
    for df in df_list
]

df_names = [
    "A_police_df",
    "A_security_light_df",
    "A_emergency_bell_df",
    "A_cctv_df",
    "J_bus_df",
    "J_subway_df",
    "R_landmarks_df",
    "R_stores_df",
]

locations_1km_dict = {
    df_name: [
        (lat, lng)
        for lat, lng in zip(filtered["lat"], filtered["lng"])
        if haversine(center_lat, center_lon, lat, lng) <= 1
    ]
    for df_name, filtered in zip(df_names, filtered_list)
}

os.makedirs("findPlace/", exist_ok=True)


print("------------------------------------------------------------------")

print("경찰서, 가로등, CCTV, 비상벨, 버스 정류장, 지하철역 키워드로 약 2주 분량의 데이터를 수집한 결과입니다.")
print("아래 링크를 통해 각 키워드별 워드클라우드를 확인하시고, 요소의 중요도를 선택하실 때 참고해 주세요.")

images = [
    "wordcloud_bus_news.png",
    "wordcloud_cctv_news.png",
    "wordcloud_emergencybell_news.png",
    "wordcloud_police_news.png",
    "wordcloud_securitylight_news.png",
    "wordcloud_subway_news.png",
]

for image in images:
    if os.path.isfile(f"findPlace/{image}"):
        os.system(f"rm findPlace/{image}")
        print(f"{image} 이미지 업데이트 준비 완료.")
    result = os.system(
        f"hdfs dfs -get /user/maria_dev/wordcloud/{image} findPlace/{image}"
    )

    if result == 0:
        print(f"{image} 이미지 업데이트 성공.")
    else:
        print(f"{image} 이미지 업데이트 실패.")

image_directory = os.path.join(os.getcwd(), "findPlace")

os.chdir(image_directory)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1)
result = sock.connect_ex(("localhost", 8888))

if result == 0:
    print("워드클라우드 이미지 링크를 출력합니다.")
else:
    subprocess.Popen(["python3.6", "-m", "http.server", "8888"])

print("이미지 파일을 확인하려면 다음 URL로 접속해 주세요: ")

print("http://34.125.180.104:8888/wordcloud_bus_news.png")
print("http://34.125.180.104:8888/wordcloud_cctv_news.png")
print("http://34.125.180.104:8888/wordcloud_emergencybell_news.png")
print("http://34.125.180.104:8888/wordcloud_police_news.png")
print("http://34.125.180.104:8888/wordcloud_securitylight_news.png")
print("http://34.125.180.104:8888/wordcloud_subway_news.png")

print("------------------------------------------------------------------")

print("각 요소의 워드클라우드를 확인하시고, 직거래 장소를 선택하기 위한 각 요소의 중요도를 설정해 주세요.")

weights = {}
weights["A_police_df"] = int(input("경찰서에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.1
weights["A_security_light_df"] = (
    int(input("가로등에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.001
)
weights["A_emergency_bell_df"] = (
    int(input("비상벨에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.013
)
weights["A_cctv_df"] = int(input("CCTV에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.004
weights["J_bus_df"] = int(input("버스 정류장에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.022
weights["J_subway_df"] = int(input("지하철에 대한 중요도를 0부터 10까지의 점수로 매겨주세요: ")) * 0.86


lat_range = np.arange(center_lat - 0.0045, center_lat + 0.0045, 0.0009)
lon_range = np.arange(center_lon - 0.006, center_lon + 0.006, 0.0012)

grid_weights = []

pbar = tqdm(total=len(lat_range) * len(lon_range), desc="Processing", ncols=80)

for lat in lat_range:
    for lon in lon_range:
        grid_center = (lat, lon)

        grid_elements = {
            df_name: [
                (lat, lng)
                for lat, lng in zip(df["lat"], df["lng"])
                if abs(grid_center[0] - lat) <= 0.00045
                and abs(grid_center[1] - lng) <= 0.0006
            ]
            for df_name, df in zip(df_names, df_list)
        }

        grid_weight = sum(
            weights[df_name] * len(elements)
            for df_name, elements in grid_elements.items()
            if df_name in weights.keys()
        )

        grid_weights.append((grid_weight, grid_center))

        pbar.update()

pbar.close()

grid_weights.sort(reverse=True)

top3_grids = grid_weights[:3]

print("\n\n")
print("------------------------------------------------------------------\n")

top3_grids = grid_weights[:3]

print("앞서 선택하신 중요도를 바탕으로 세 군데의 구역을 선택했습니다\n")
print("-----------------------------------------------------------\n")

for i, (weight, (lat, lon)) in enumerate(top3_grids):
    print("-------------------------------------------------------\n")
    print(f"{i+1}번째 구역의 직거래 장소 추천 결과입니다.")
    grid_elements = {
        df_name: df[
            (abs(df["lat"] - lat) <= 0.00045) & (abs(df["lng"] - lon) <= 0.0006)
        ]
        for df_name, df in zip(df_names, df_list)
    }

    for df_name, elements in grid_elements.items():
        if not elements.empty:
            img_land = []
            img_store = []

            if df_name == "A_police_df":
                img_pol = []
                names = []

                for _, row in elements.iterrows():
                    names.append(f"{row['경찰서']}경찰서")
                    img_pol.append(row["img"])

                print(f"\n경찰서는 {', '.join(names)}를 추천합니다.")

                for img in img_pol:
                    if os.path.isfile(f"resultImage/PoliceImg/{img}.jpg"):
                        os.remove(f"resultImage/PoliceImg/{img}.jpg")
                    result = os.system(
                        f"hdfs dfs -get /user/maria_dev/findPlaceData/PoliceImg/{img}.jpg resultImage/PoliceImg/{img}.jpg"
                    )

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(("localhost", 8888))

                if result == 1:
                    subprocess.Popen(["python3.6", "-m", "http.server", "8888"])
                for img in img_pol:
                    print(f"http://34.125.180.104:8888/resultImage/PoliceImg/{img}.jpg")

            elif df_name == "J_bus_df":
                names = []
                img_bus = []

                for _, row in elements.iterrows():
                    names.append(row["정류소명"])
                    img_bus.append(row["img"])

                print(f"\n버스 정류장은 {', '.join(names)}정류장을 추천합니다.")

                for img in img_bus:
                    if os.path.isfile(f"resultImage/BusImg/{img}.jpg"):
                        os.remove(f"resultImage/BusImg/{img}.jpg")

                    result = os.system(
                        f"hdfs dfs -get /user/maria_dev/findPlaceData/BusImg/{img}.jpg resultImage/BusImg/{img}.jpg"
                    )

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(("localhost", 8888))

                if result == 1:
                    subprocess.Popen(["python3.6", "-m", "http.server", "8888"])
                for img in img_bus:
                    print(f"http://34.125.180.104:8888/resultImage/BusImg/{img}.jpg")

            elif df_name == "J_subway_df":
                if df_name == "J_subway_df":
                    names = []
                    img_sub = []

                    for _, row in elements.iterrows():
                        names.append(str(row["호선"]) + "호선 " + row["역명"])
                        img_sub.append(row["img"])

                    print(f"\n지하철역은 {', '.join(names)}를 추천합니다.")

                    for img in img_sub:
                        if os.path.isfile(f"resultImage/SubwayImg/{img}.jpg"):
                            os.remove(f"resultImage/SubwayImg/{img}.jpg")

                        result = os.system(
                            f"hdfs dfs -get /user/maria_dev/findPlaceData/SubwayImg/{img}.jpg resultImage/SubwayImg/{img}.jpg"
                        )

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex(("localhost", 8888))

                    if result == 1:
                        subprocess.Popen(["python3.6", "-m", "http.server", "8888"])

                    for img in img_sub:
                        print(
                            f"http://34.125.180.104:8888/resultImage/SubwayImg/{img}.jpg"
                        )

                elif df_name == "R_landmarks_df":
                    print("\n저희가 추천하는 직거래 장소(랜드마크)는 다음과 같습니다.")
                    names = []

                    for _, row in elements.iterrows():
                        names.append(f"{row['name']}")
                        lat = row["lat"]
                        lng = row["lng"]

                        url = f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=80&heading=70&pitch=0&key=AIzaSyDBzW7GW5eHhpayqNSZfpeiN4H0R3e4_fQ"

                        response = requests.get(url)

                        if response.status_code == 200:
                            file_name = str(row["imgId"]) + ".jpg"
                            with open(f"resultImage/LandImg/{file_name}", "wb") as file:
                                file.write(response.content)
                            print(row["name"])
                            print(
                                f"http://34.125.180.104:8888/resultImage/LandImg/{file_name}\n"
                            )

                elif df_name == "R_stores_df":
                    print("\n저희가 추천하는 직거래 장소(상가)는 다음과 같습니다.")
                    names = []

                    for _, row in elements.sample(n=5).iterrows():
                        names.append(row["상호명"])
                        lat = row["lat"]
                        lng = row["lng"]

                        url = f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=80&heading=70&pitch=0&key=AIzaSyDBzW7GW5eHhpayqNSZfpeiN4H0R3e4_fQ"

                        response = requests.get(url)

                        if response.status_code == 200:
                            file_name = row["img"] + ".jpg"
                            with open(
                                f"resultImage/StoreImg/{file_name}", "wb"
                            ) as file:
                                file.write(response.content)
                            print(row["상호명"])
                            print(
                                f"http://34.125.180.104:8888/resultImage/StoreImg/{file_name}\n"
                            )
