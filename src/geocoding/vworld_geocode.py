import requests
import os
from dotenv import load_dotenv

# .env에서 API 키 로드
load_dotenv()
VWORLD_API_KEY = os.getenv("VWORLD_API_KEY")

def road_address_to_coordinates(road_addr: str) -> tuple:
    """
    도로명 주소 → (경도, 위도) 좌표 반환

    Args:
        road_addr (str): 예) '서울특별시 종로구 율곡로 283'

    Returns:
        (longitude, latitude): tuple or (None, None)
    """
    try:
        url = "https://apis.vworld.kr/new2coord.do"
        params = {
            "q": road_addr,
            "output": "json",
            "epsg": "epsg:4326",
            "apiKey": VWORLD_API_KEY
        }
        response = requests.get(url, params=params)
        data = response.json()
        # print("응답 내용:", response.text)

        if response.status_code == 200 and 'EPSG_4326_X' in data and 'EPSG_4326_Y' in data:
            return float(data['EPSG_4326_X']), float(data['EPSG_4326_Y'])
        else:
            print(f"[주소→좌표 실패] {road_addr}")
            return None, None
    except Exception as e:
        print(f"[예외 발생 - 주소→좌표] {e}")
        return None, None


def coordinates_to_jibun_address(longitude: float, latitude: float) -> str:
    """
    좌표 → 지번주소 반환 (VWorld API 사용)
    """
    try:
        url = "https://apis.vworld.kr/coord2jibun.do"
        params = {
            "x": longitude,
            "y": latitude,
            "output": "json",
            "epsg": "epsg:4326",
            "apiKey": VWORLD_API_KEY
        }
        response = requests.get(url, params=params)
        print("지번 응답:", response.text)  # 디버깅용

        data = response.json()

        addr = data.get("ADDR", None)
        if response.status_code == 200 and addr:
            return addr
        else:
            print(f"[좌표→지번주소 실패] ({longitude}, {latitude})")
            return None
    except Exception as e:
        print(f"[예외 발생 - 좌표→지번주소] {e}")
        return None


def coordinates_to_road_address(longitude: float, latitude: float) -> str:
    """
    위도/경도 → 도로명주소 반환 (VWorld Geocoder API 사용)
    """
    api_url = "https://api.vworld.kr/req/address"
    params = {
        "service": "address",
        "request": "getAddress",
        "format": "json",
        "type": "ROAD",  # 도로명주소 반환
        "point": f"{longitude},{latitude}",
        "key": VWORLD_API_KEY
    }

    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        result = response.json()
        address_info = result.get("response", {}).get("result", [])
        if address_info:
            return address_info[0].get("text")
        else:
            print(f"[도로명주소 없음] ({longitude}, {latitude})")
            return None
    else:
        print(f"[API 오류] status: {response.status_code}")
        return None

# 도로명 -> 지번
def road_to_jibun_address(road_address: str) -> str:
    try:
        url = "https://apis.vworld.kr/addr2jibun.do"
        params = {
            "addr": road_address,
            "output": "json",
            "apiKey": VWORLD_API_KEY
        }
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", {})
            jibun_addr = result.get("jibunAddr")
            if jibun_addr and jibun_addr.strip() and jibun_addr != "검색결과가 없습니다":
                return jibun_addr.strip()
            else:
                print(f"[변환 실패] '{road_address}' → 결과 없음 또는 무효: {jibun_addr}")
        else:
            print(f"[HTTP 오류] {road_address} → {response.status_code}, 응답: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[네트워크 오류] {road_address} → {e}")
        return None
    except Exception as e:
        print(f"[기타 오류] {road_address} → {e}")
        return None

    
# # 순서 : 경도 위도
# # 1. 도로명 주소 → 좌표
# lon, lat = road_address_to_coordinates("서울특별시 종로구 율곡로 283")
# print("좌표:", lon, lat)

# # 2. 좌표 → 지번주소
# if lon and lat:
#     jibun = coordinates_to_jibun_address(lon, lat)
#     print("지번주소:", jibun)
