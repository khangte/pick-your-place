"""
📌 서울시 GeoJSON 기준 위경도 → 행정동 공간 조인 후 개수 집계
- 파일 리스트를 순차적으로 처리
- 위경도 컬럼 자동 감지
- sjoin을 활용한 고속 공간 매핑
- 실패한 행은 별도로 저장 (e.g., __failed.csv)
- 서울 외 지역도 식별하여 제외 (예: subway_station)
- 상세 처리 시간 기록 포함

🧪 개발환경: PyCharm 실행 기준
"""

import os
import sys
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from datetime import datetime

# 🕐 실행 시작 시간 기록
script_start_time = time.time()
start_datetime = datetime.now()

print(f"🚀 공간 조인 스크립트 실행 시작: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

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
print("📍 서울시 GeoJSON 로딩 중...")
geojson_start = time.time()
gdf = gpd.read_file(GEOJSON_PATH).to_crs(epsg=4326)
geojson_load_time = time.time() - geojson_start
print(f"✅ 서울시 GeoJSON 로딩 완료 ({geojson_load_time:.2f}초)")

# ✅ 전체 결과 리스트
summary_list = []
total_processed_files = 0
total_processed_rows = 0
total_mapped_rows = 0

# ✅ 전체 시작 시간
total_start = time.time()

for filename in TARGET_FILES:
    file_start_time = time.time()
    print(f"\n[🔄 처리 중] {filename}")
    file_path = os.path.join(INPUT_DIR, filename)

    # ✅ 파일 읽기
    try:
        read_start = time.time()
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
        read_time = time.time() - read_start
        print(f"   📖 파일 읽기 완료 ({read_time:.2f}초)")
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
    geometry_start = time.time()
    df[lon_col] = df[lon_col].astype(float)
    df[lat_col] = df[lat_col].astype(float)

    df["geometry"] = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf_points = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    geometry_time = time.time() - geometry_start
    print(f"   🗺️ 지오메트리 생성 완료 ({geometry_time:.2f}초)")

    # ✅ 공간 조인
    join_start = time.time()
    joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")
    join_time = time.time() - join_start
    print(f"   🔗 공간 조인 완료 ({join_time:.2f}초)")

    # ✅ 서울 여부 확인 (subway_station만 예외 처리)
    if "subway_station" in filename:
        print("   🚇 지하철역 - 전국 GeoJSON으로 서울 외 지역 필터링 중...")
        full_join_start = time.time()
        full_gdf = gpd.read_file(FULL_GEOJSON_PATH).to_crs(epsg=4326)
        full_joined = gpd.sjoin(gdf_points, full_gdf, how="left", predicate="within")
        joined["sidonm"] = full_joined["sidonm"]

        # 서울 아닌 경우 실패 처리
        failed_rows = joined[joined["sidonm"] != "서울특별시"]
        joined = joined[joined["sidonm"] == "서울특별시"]
        full_join_time = time.time() - full_join_start
        print(f"   🌏 전국 조인 및 필터링 완료 ({full_join_time:.2f}초)")
    else:
        failed_rows = joined[joined["adm_nm"].isna()]
        joined = joined[~joined["adm_nm"].isna()]

    # ✅ 컬럼 분리 및 집계
    processing_start = time.time()
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
    processing_time = time.time() - processing_start
    print(f"   📊 데이터 처리 및 집계 완료 ({processing_time:.2f}초)")

    # ✅ 저장
    save_start = time.time()
    name_prefix = filename.replace("__raw.csv", "").replace("__raw.xlsx", "")
    counts_path = os.path.join(OUTPUT_DIR, f"{name_prefix}__counts.csv")
    counts.to_csv(counts_path, index=False, encoding="utf-8-sig")

    # ✅ 실패 데이터 저장
    if not failed_rows.empty:
        failed_path = os.path.join(OUTPUT_DIR, f"{name_prefix}__failed.csv")
        failed_rows.drop(columns=["index_right"], errors="ignore").to_csv(
            failed_path, index=False, encoding="utf-8-sig"
        )
        print(f"   ⚠️ 실패 데이터 저장: {failed_path} ({len(failed_rows)} rows)")

    save_time = time.time() - save_start
    print(f"   💾 파일 저장 완료 ({save_time:.2f}초)")

    # ✅ 로그 출력 및 요약 저장
    file_elapsed = time.time() - file_start_time
    success_rate = len(joined) / len(df) * 100 if len(df) > 0 else 0

    print(f"📊 전체 행 수: {len(df):,}")
    print(f"✅ 매핑 성공 행 수: {len(joined):,}")
    print(f"🎯 매핑 성공률: {success_rate:.2f}%")
    print(f"⏱️ 총 파일 처리 시간: {file_elapsed:.2f}초")

    # 단계별 시간 비율 계산
    print(f"   └─ 읽기: {read_time:.2f}초 ({read_time/file_elapsed*100:.1f}%)")
    print(f"   └─ 지오메트리: {geometry_time:.2f}초 ({geometry_time/file_elapsed*100:.1f}%)")
    print(f"   └─ 조인: {join_time:.2f}초 ({join_time/file_elapsed*100:.1f}%)")
    if "subway_station" in filename:
        print(f"   └─ 전국조인: {full_join_time:.2f}초 ({full_join_time/file_elapsed*100:.1f}%)")
    print(f"   └─ 처리: {processing_time:.2f}초 ({processing_time/file_elapsed*100:.1f}%)")
    print(f"   └─ 저장: {save_time:.2f}초 ({save_time/file_elapsed*100:.1f}%)")

    summary_list.append({
        "filename": filename,
        "total_rows": len(df),
        "mapped_rows": len(joined),
        "failed_rows": len(failed_rows),
        "success_rate": round(success_rate, 2),
        "time_sec": round(file_elapsed, 2),
        "read_time": round(read_time, 2),
        "geometry_time": round(geometry_time, 2),
        "join_time": round(join_time, 2),
        "processing_time": round(processing_time, 2),
        "save_time": round(save_time, 2)
    })

    total_processed_files += 1
    total_processed_rows += len(df)
    total_mapped_rows += len(joined)

# ✅ 전체 처리 시간 계산
total_elapsed = time.time() - total_start
script_total_time = time.time() - script_start_time
end_datetime = datetime.now()

# 시간 포맷팅
hours = int(script_total_time // 3600)
minutes = int((script_total_time % 3600) // 60)
seconds = int(script_total_time % 60)

print(f"\n" + "="*70)
print(f"🏁 스크립트 실행 완료: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📊 처리 통계:")
print(f"   - 처리된 파일 수: {total_processed_files}개")
print(f"   - 총 처리 행 수: {total_processed_rows:,}개")
print(f"   - 총 매핑 성공 행 수: {total_mapped_rows:,}개")
print(f"   - 전체 성공률: {total_mapped_rows/total_processed_rows*100:.2f}%")
print(f"   - 데이터 처리 시간: {total_elapsed:.2f}초")
print(f"   - 전체 실행 시간: {hours:02d}:{minutes:02d}:{seconds:02d} ({script_total_time:.2f}초)")
print(f"   - 평균 처리 속도: {total_processed_rows/total_elapsed:.1f} 행/초")
print(f"   - GeoJSON 로딩 시간: {geojson_load_time:.2f}초 ({geojson_load_time/script_total_time*100:.1f}%)")
print("="*70)

# ✅ 요약 저장
summary_df = pd.DataFrame(summary_list)
summary_path = os.path.join(OUTPUT_DIR, "__summary.csv")
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"\n📋 전체 요약 저장 완료 → {summary_path}")

# ✅ 상세 시간 분석 출력
print(f"\n📈 단계별 평균 처리 시간 분석:")
if len(summary_list) > 0:
    avg_read = sum(s['read_time'] for s in summary_list) / len(summary_list)
    avg_geometry = sum(s['geometry_time'] for s in summary_list) / len(summary_list)
    avg_join = sum(s['join_time'] for s in summary_list) / len(summary_list)
    avg_processing = sum(s['processing_time'] for s in summary_list) / len(summary_list)
    avg_save = sum(s['save_time'] for s in summary_list) / len(summary_list)

    print(f"   - 파일 읽기: {avg_read:.2f}초")
    print(f"   - 지오메트리 생성: {avg_geometry:.2f}초")
    print(f"   - 공간 조인: {avg_join:.2f}초")
    print(f"   - 데이터 처리: {avg_processing:.2f}초")
    print(f"   - 파일 저장: {avg_save:.2f}초")