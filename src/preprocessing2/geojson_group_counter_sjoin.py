"""
📌 서울시 GeoJSON 기준 위경도 → 행정동 공간 조인 후 개수 집계
- 파일 리스트를 순차적으로 처리
- 위경도 컬럼 자동 감지
- sjoin을 활용한 고속 공간 매핑
- 실패한 행은 별도로 저장 (e.g., __failed.csv)
- 서울 외 지역도 식별하여 제외 (예: subway_station)
- 처리 시간 기록 포함

🧪 개발환경: PyCharm 실행 기준
"""

import os
import sys
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# 🔐 항상 프로젝트 루트를 기준으로 상대 경로가 맞춰지도록 설정
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
os.chdir(project_root)
sys.path.append(os.path.join(project_root, "src"))

# ✅ 경로 설정
INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/processed_counts2"
GEOJSON_PATH = "data/reference/Seoul_HangJeongDong.geojson"
FULL_GEOJSON_PATH = "data/reference/HangJeongDong_ver20250401.geojson"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ 처리 대상 파일 목록
TARGET_FILES = [
    "bus_stop__raw.csv",
    "cctv__raw.csv",
    "hospital__raw.csv",
    "library__raw.csv",
    "park__raw.csv",
    "safety_bell__raw.xlsx",
    "street_light__raw.csv",
    "subway_station__raw.csv"
]

# ✅ GeoJSON 로드
gdf = gpd.read_file(GEOJSON_PATH).to_crs(epsg=4326)

# ✅ 전체 결과 리스트
summary_list = []

# ✅ 전체 시작 시간
total_start = time.time()

for filename in TARGET_FILES:
    start_time = time.time()
    print(f"\n[🔄 처리 중] {filename}")
    file_path = os.path.join(INPUT_DIR, filename)

    # ✅ 파일 읽기
    try:
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(file_path, dtype=str)
            except UnicodeDecodeError:
                print(f"[⚠️ 인코딩 재시도] {filename} → cp949")
                df = pd.read_csv(file_path, dtype=str, encoding="cp949")
        elif filename.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str)
        else:
            print(f"[❌ 지원되지 않는 형식] {filename}")
            continue
    except Exception as e:
        print(f"[❌ 읽기 실패] {filename} → {e}")
        continue

    # ✅ 경도/위도 컬럼 자동 탐색
    lon_col = next((col for col in df.columns if col.lower() in ["경도", "longitude", "xcrd", "병원경도", "wgs84경도"]), None)
    lat_col = next((col for col in df.columns if col.lower() in ["위도", "latitude", "ycrd", "병원위도", "wgs84위도"]), None)

    if not lon_col or not lat_col:
        print(f"[❌ 경위도 컬럼 없음] {filename}")
        continue

    # ✅ 위경도 → Point 객체 생성
    df[lon_col] = df[lon_col].astype(float)
    df[lat_col] = df[lat_col].astype(float)
    df["geometry"] = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf_points = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # ✅ 공간 조인
    joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")

    # ✅ 서울 여부 확인 (subway_station만 예외 처리)
    if "subway_station" in filename:
        full_gdf = gpd.read_file(FULL_GEOJSON_PATH).to_crs(epsg=4326)
        full_joined = gpd.sjoin(gdf_points, full_gdf, how="left", predicate="within")
        joined["sidonm"] = full_joined["sidonm"]

        # 서울 아닌 경우 실패 처리
        failed_rows = joined[joined["sidonm"] != "서울특별시"]
        joined = joined[joined["sidonm"] == "서울특별시"]
    else:
        failed_rows = joined[joined["adm_nm"].isna()]
        joined = joined[~joined["adm_nm"].isna()]

    # ✅ 컬럼 분리 및 집계
    joined["gu_name"] = joined["adm_nm"].str.split().str[1]
    joined["dong_name"] = joined["adm_nm"].str.split().str[2]
    joined["gu_code"] = joined["adm_cd2"].str[:5]
    joined["dong_code"] = joined["adm_cd2"]

    counts = (
        joined.groupby(["gu_code", "dong_code", "gu_name", "dong_name"])
        .size()
        .reset_index(name="counts")
        .sort_values("counts", ascending=False)
    )

    # ✅ 저장
    name_prefix = filename.replace("__raw.csv", "").replace("__raw.xlsx", "")
    counts_path = os.path.join(OUTPUT_DIR, f"{name_prefix}__counts.csv")
    counts.to_csv(counts_path, index=False, encoding="utf-8-sig")
    print(f"[💾 저장 완료] {counts_path}")

    # ✅ 실패 데이터 저장
    if not failed_rows.empty:
        failed_path = os.path.join(OUTPUT_DIR, f"{name_prefix}__failed.csv")
        failed_rows.drop(columns=["index_right"], errors="ignore").to_csv(
            failed_path, index=False, encoding="utf-8-sig"
        )
        print(f"[⚠️ 실패 데이터 저장] {failed_path} ({len(failed_rows)} rows)")

    # ✅ 로그 출력 및 요약 저장
    elapsed = time.time() - start_time
    print(f"📊 전체 행 수: {len(df)}")
    print(f"✅ 매핑 성공 행 수: {len(joined)}")
    print(f"🎯 매핑 성공률: {len(joined) / len(df) * 100:.2f}%")
    print(f"⏱️ 처리 시간: {elapsed:.2f}초")

    summary_list.append({
        "filename": filename,
        "total_rows": len(df),
        "mapped_rows": len(joined),
        "success_rate": round(len(joined) / len(df) * 100, 2),
        "time_sec": round(elapsed, 2)
    })

# ✅ 전체 처리 시간
total_elapsed = time.time() - total_start
print(f"\n✅ 전체 처리 완료! 총 소요 시간: {total_elapsed:.2f}초")

# ✅ 요약 저장
summary_df = pd.DataFrame(summary_list)
summary_path = os.path.join(OUTPUT_DIR, "__summary.csv")
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"\n📋 전체 요약 저장 완료 → {summary_path}")

