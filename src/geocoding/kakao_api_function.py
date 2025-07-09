import os
import requests
from dotenv import load_dotenv
from typing import Any

load_dotenv()
API_KEY = os.getenv("KAKAO_API_KEY")

def coord_to_region(longitude: float, latitude: float) -> tuple[Any, Any, Any] | None:
    """
    위도/경도로 주소(도로명 주소)를 반환하는 함수 (카카오 API 사용)
    """
    headers = {"Authorization": f"KakaoAK {API_KEY}"}
    url = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
    params = {"x": longitude, "y": latitude}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        if response.status_code != 200:
            print(f"[❌ API 실패] status={response.status_code}, 내용: {response.text}")
            return None

        documents = response.json().get("documents", [])
        if not documents:
            print("⚠️ 결과 없음")
            return None

        # 🎯 행정동(H) 우선, 없으면 법정동(B) 사용
        for doc in documents:
            if doc.get("region_type") == "H":
                return (
                    doc["region_1depth_name"],
                    doc["region_2depth_name"],
                    doc["region_3depth_name"]
                )

        # fallback: B (법정동)
        doc = documents[0]
        return (
            doc["region_1depth_name"],
            doc["region_2depth_name"],
            doc["region_3depth_name"]
        )

    except Exception as e:
        print(f"[예외 발생] {e}")
        return None


def address_to_coord(address: str) -> tuple[float, float] | None:
    """
    주소 → 위도, 경도 변환
    :param address: 예: "전북 삼성동 100"
    :return: (longitude, latitude) 튜플 또는 None
    """
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {API_KEY}"}
    params = {"query": address}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            result = response.json()
            documents = result.get("documents", [])
            if documents:
                x = float(documents[0]["x"])  # 경도
                y = float(documents[0]["y"])  # 위도
                return x, y
            else:
                print("⚠️ 주소 검색 결과 없음")
        else:
            print(f"[API 오류] status={response.status_code} → {response.text}")
    except Exception as e:
        print(f"[예외 발생] {e}")

    return None


def coord_to_address(longitude: float, latitude: float) -> str:
    """
    위도/경도로 주소(도로명 주소)를 반환하는 함수 (카카오 API 사용)
    """
    headers = {"Authorization": f"KakaoAK {API_KEY}"}
    url = "https://dapi.kakao.com/v2/local/geo/coord2address.json"
    params = {"x": longitude, "y": latitude}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            address = data['documents'][0]['address']['address_name']
            return address
        else:
            print(f"[요청 실패] status_code={response.status_code}")
            return None
    except Exception as e:
        print(f"[예외 발생] {e}")
        return None
