# src/preprocessing2/mapping_utils.py

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def perform_geojson_mapping(
    input_path: str,
    output_dir: str,
    geojson_path: str,
    full_geojson_path: str,
    filename: str,
) -> dict:
    """
    GeoJSON 기반 행정동 매핑 + 집계 + 실패 저장 함수

    Returns: summary dict
    """
    file_path = os.path.join(input_path, filename)
    name_prefix = filename.replace("__raw.csv", "").replace("__raw.xlsx", "")

    # 1. 파일 읽기
    try:
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(file_path, dtype=str)
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, dtype=str, encoding="cp949")
        elif filename.endswith(".xlsx"):
            df = pd.read_excel(file_path, dtype=str)
        else:
            return {"filename": filename, "error": "Unsupported file type"}
    except Exception as e:
        return {"filename": filename, "error": str(e)}

    # 2. 위도/경도 컬럼 탐색
    lon_col = next((c for c in df.columns if c.lower() in ["경도", "longitude", "xcrd", "병원경도", "wgs84경도"]), None)
    lat_col = next((c for c in df.columns if c.lower() in ["위도", "latitude", "ycrd", "병원위도", "wgs84위도"]), None)

    if not lon_col or not lat_col:
        return {"filename": filename, "error": "Missing lat/lon columns"}

    # 3. GeoDataFrame 생성
    df[lon_col] = df[lon_col].astype(float)
    df[lat_col] = df[lat_col].astype(float)
    df["geometry"] = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf_points = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # 4. 서울 GeoJSON 로드
    gdf = gpd.read_file(geojson_path).to_crs(epsg=4326)

    # 5. 공간조인
    joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")

    # 6. 예외처리: subway_station이면 서울만 필터링
    if "subway_station" in filename:
        full_gdf = gpd.read_file(full_geojson_path).to_crs(epsg=4326)
        full_joined = gpd.sjoin(gdf_points, full_gdf, how="left", predicate="within")
        joined["sidonm"] = full_joined["sidonm"]
        failed_rows = joined[joined["sidonm"] != "서울특별시"]
        joined = joined[joined["sidonm"] == "서울특별시"]
    else:
        failed_rows = joined[joined["adm_nm"].isna()]
        joined = joined[~joined["adm_nm"].isna()]

    # 7. 컬럼 분해 및 집계
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

    os.makedirs(output_dir, exist_ok=True)
    counts.to_csv(os.path.join(output_dir, f"{name_prefix}__counts.csv"), index=False, encoding="utf-8-sig")

    if not failed_rows.empty:
        failed_rows.drop(columns=["index_right"], errors="ignore").to_csv(
            os.path.join(output_dir, f"{name_prefix}__failed.csv"), index=False, encoding="utf-8-sig"
        )

    return {
        "filename": filename,
        "total_rows": len(df),
        "mapped_rows": len(joined),
        "success_rate": round(len(joined) / len(df) * 100, 2),
        "counts_df": counts,
        "mapped_df": joined[["gu_name", "dong_name", "gu_code", "dong_code"]].copy()
    }

