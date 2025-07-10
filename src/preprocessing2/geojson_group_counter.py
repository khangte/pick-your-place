"""
📌 GeoJSON 기반 위경도 → 행정동 매핑 및 개수 집계 스크립트

이 스크립트는 서울시 행정동 GeoJSON 데이터를 이용하여
data/raw/ 폴더 내 지정된 원시 데이터(*__raw.csv 또는 .xlsx)의
위도(latitude), 경도(longitude) 값을 기반으로
각 자치구/행정동 단위로 매핑 및 개수 집계를 수행합니다.

---

🔧 주요 기능:
- 위경도 → 행정동 매핑 (GeoJSON 기반 공간 포함 여부 판별)
- 자치구코드(gu_code), 행정동코드(dong_code) 기준 개수 집계
- 자치구명/동명은 보조정보로 포함
- 처리 요약 (__summary.csv) 저장 포함
- 전체 실행시간 측정 및 표시

📂 입력 경로:
- 데이터:        data/raw/*__raw.csv 또는 *__raw.xlsx
- 참조 GeoJSON:  data/reference/Seoul_HangJeongDong.geojson

📁 출력 경로:
- 결과 CSV:     data/processed_counts2/{파일명__counts.csv}
- 처리 요약:    data/processed_counts2/__summary.csv

⚠️ 실행 주의:
- PyCharm에서 실행 시, 기본 working directory가 src/preprocessing2로 설정됨.
  따라서 경로 오류를 방지하기 위해 os.chdir를 통해 프로젝트 루트 기준으로 변경하는 코드 포함.
- 다양한 컬럼명("위도", "LATITUDE", "병원위도" 등)을 자동 감지하여 처리.

✅ 실행 환경: **PyCharm 실행**

작성자: 강민혁
"""
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import time
from datetime import datetime

# 🕐 실행 시작 시간 기록
start_time = time.time()
start_datetime = datetime.now()

print(f"🚀 스크립트 실행 시작: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

# 🔐 항상 프로젝트 루트를 기준으로 상대 경로가 맞춰지도록 설정
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
os.chdir(project_root)
sys.path.append(os.path.join(project_root, "src"))

# ✅ 경로 설정
RAW_DIR = "data/raw"
OUTPUT_DIR = "data/processed_counts2"
GEOJSON_PATH = "data/reference/Seoul_HangJeongDong.geojson"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ 처리할 파일 리스트
TARGET_FILES = [
    "bus_stop__raw.csv",
    "cctv__raw.csv",
    "hospital__raw.csv",
    "library__raw.csv",
    "park__raw.csv",
    "safety_bell__raw.xlsx",  # 엑셀도 포함됨
    "street_light__raw.csv",
    "subway_station__raw.csv"
]

# ✅ GeoJSON 로딩
print("📍 GeoJSON 로딩 중...")
geojson_start = time.time()
gdf = gpd.read_file(GEOJSON_PATH).to_crs(epsg=4326)
geojson_load_time = time.time() - geojson_start
print(f"✅ GeoJSON 로딩 완료 ({geojson_load_time:.2f}초)")

# ✅ GeoJSON 기반 동 정보 추출 함수
def get_dong_info_by_coords(lon, lat):
    try:
        point = Point(float(lon), float(lat))
        for _, row in gdf.iterrows():
            if row["geometry"].contains(point):
                props = row["properties"] if "properties" in row else row
                return {
                    "gu_code": props["sgg"],
                    "dong_code": props["adm_cd2"],
                    "gu_name": props["sggnm"],
                    "dong_name": props["adm_nm"].split()[-1],
                }
        return None  # 포함 안됨
    except Exception as e:
        print(f"[❌ 매핑 오류] 위도: {lat}, 경도: {lon} → {e}")
        return None

# ✅ tqdm 설정
tqdm.pandas()

summary_rows = []
total_processed_files = 0
total_processed_rows = 0

# ✅ 각 파일 반복 처리
for filename in TARGET_FILES:
    input_path = os.path.join(RAW_DIR, filename)
    if not os.path.exists(input_path):
        print(f"[❌ 없음] {filename} → 건너뜀")
        continue

    print(f"\n[🔄 처리 중] {filename}")
    file_start_time = time.time()

    # ✅ 파일 확장자 구분
    try:
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(input_path, dtype=str, encoding="utf-8")
            except UnicodeDecodeError:
                print(f"[⚠️ 인코딩 재시도] {filename} → cp949로 다시 시도")
                df = pd.read_csv(input_path, dtype=str, encoding="cp949")
        elif filename.endswith(".xlsx"):
            df = pd.read_excel(input_path, dtype=str)
        else:
            print(f"[⚠️ 확장자 미지원] {filename}")
            continue
    except Exception as e:
        print(f"[❌ 읽기 실패] {filename} → {e}")
        continue

    total_rows = len(df)
    total_processed_rows += total_rows

    # ✅ 경도/위도 컬럼 자동 감지 (사용자 지정 후보 포함)
    lon_col = next(
        (col for col in df.columns if col in ["경도", "LONGITUDE", "longitude", "XCRD", "병원경도", "WGS84경도"])
        , None
    )
    lat_col = next(
        (col for col in df.columns if col in ["위도", "LATITUDE", "latitude", "YCRD", "병원위도", "WGS84위도"])
        , None
    )

    if not lon_col or not lat_col:
        print(f"[⚠️ 스킵] {filename} → 위도/경도 컬럼 없음")
        summary_rows.append({
            "filename": filename,
            "total_rows": total_rows,
            "matched_rows": 0,
            "success_rate": 0.0,
            "processing_time": 0.0
        })
        continue

    # ✅ 동 정보 추출
    mapping_start = time.time()
    info_series = df[[lon_col, lat_col]].progress_apply(
        lambda row: get_dong_info_by_coords(row[lon_col], row[lat_col]) or {}, axis=1
    )
    mapping_time = time.time() - mapping_start

    info_df = pd.DataFrame(info_series.tolist())
    result_df = pd.concat([df, info_df], axis=1)

    # ✅ 유효 매핑 필터링
    matched_df = result_df.dropna(subset=["gu_code", "dong_code"])
    matched_rows = len(matched_df)
    success_rate = (matched_rows / total_rows) * 100 if total_rows > 0 else 0

    file_processing_time = time.time() - file_start_time

    print(f"📊 전체 행 수: {total_rows}")
    print(f"✅ 매핑 성공 행 수: {matched_rows}")
    print(f"🎯 매핑 성공률: {success_rate:.2f}%")
    print(f"⏱️ 파일 처리 시간: {file_processing_time:.2f}초 (매핑: {mapping_time:.2f}초)")

    summary_rows.append({
        "filename": filename,
        "total_rows": total_rows,
        "matched_rows": matched_rows,
        "success_rate": round(success_rate, 2),
        "processing_time": round(file_processing_time, 2)
    })

    if matched_rows == 0:
        print(f"[⚠️ 스킵] {filename} → 매핑된 데이터 없음")
        continue

    # ✅ 자치구코드 + 행정동코드 기준 개수 집계
    count_df = (
        matched_df.groupby(["gu_code", "dong_code", "gu_name", "dong_name"])
        .size()
        .reset_index(name="counts")
    )

    # ✅ 저장
    output_filename = filename.replace("__raw.xlsx", "__counts.csv").replace("__raw.csv", "__counts.csv")
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    count_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[💾 저장 완료] {output_path}")

    total_processed_files += 1

# ✅ 전체 요약 저장
summary_df = pd.DataFrame(summary_rows)
summary_path = os.path.join(OUTPUT_DIR, "__summary.csv")
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"\n📋 처리 요약 저장 완료 → {summary_path}")

# 🕐 실행 종료 시간 및 총 소요 시간 계산
end_time = time.time()
end_datetime = datetime.now()
total_execution_time = end_time - start_time

# 시간 포맷팅 (시:분:초)
hours = int(total_execution_time // 3600)
minutes = int((total_execution_time % 3600) // 60)
seconds = int(total_execution_time % 60)

print(f"\n" + "="*60)
print(f"🏁 스크립트 실행 완료: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📊 처리 통계:")
print(f"   - 처리된 파일 수: {total_processed_files}개")
print(f"   - 총 처리 행 수: {total_processed_rows:,}개")
print(f"   - 전체 실행 시간: {hours:02d}:{minutes:02d}:{seconds:02d} ({total_execution_time:.2f}초)")
print(f"   - 평균 처리 속도: {total_processed_rows/total_execution_time:.1f} 행/초")
print("="*60)
