import requests
import pandas as pd
import json

API_KEY = '696c645274746869353949594b5869'
SERVICE_NAME = 'neisSchoolInfo'
BASE_URL = 'http://openapi.seoul.go.kr:8088'

START_INDEX = 1
END_INDEX = 1000

def build_url(api_key, service, start, end):
    return f"{BASE_URL}/{api_key}/json/{service}/{start}/{end}"

def fetch_school_info():
    url = build_url(API_KEY, SERVICE_NAME, START_INDEX, END_INDEX)
    print(f"📡 요청 URL: {url}")
    
    response = requests.get(url)
    print(f"🔍 응답 코드: {response.status_code}")
    
    if response.status_code == 200:
        try:
            data = response.json()
            if SERVICE_NAME not in data:
                print("❌ 응답 내에 SERVICE_NAME 키가 없음")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                return

            if 'row' not in data[SERVICE_NAME]:
                print("❌ 'row' 키가 없음")
                print(json.dumps(data[SERVICE_NAME], indent=2, ensure_ascii=False))
                return

            rows = data[SERVICE_NAME]['row']
            df = pd.DataFrame(rows)
            print("📂 총 행 수:", len(df))
            print("📌 컬럼:", df.columns.tolist())
            print(df.head())

            # 원하는 컬럼만 추출
            df_selected = df[['SCHUL_KND_SC_NM', 'SCHUL_NM', 'ORG_RDNMA']]
            print("🔍 Null 수:\n", df_selected.isnull().sum())
            df_selected = df_selected.dropna()
            df_selected.to_csv("서울_학교요약.csv", index=False, encoding='utf-8-sig')
            print("✅ 저장 완료: 서울_학교요약.csv")
            return df_selected
        except Exception as e:
            print("❌ JSON 파싱 실패:", e)
            print(response.text[:300])
    else:
        print(f"❌ 요청 실패: {response.status_code}")
        print(response.text)

# 실행
fetch_school_info()
