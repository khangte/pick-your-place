import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from kakao_api_function import reverse_geocode
from vworld_geocode import road_address_to_coordinates, coordinates_to_jibun_address, coordinates_to_road_address
from admin_mapper import extract_gu_and_dong, get_gu_dong_codes

print("===")

# Step 1. 도로명 주소 → 좌표
lon, lat = road_address_to_coordinates("서울 종로구 율곡로 283")
print("좌표:", lon, lat)

road1 = reverse_geocode(lon, lat)
print("카카오 지번주소:", road1)

# Step 2. 좌표 → 지번주소
jibun = coordinates_to_jibun_address(lon, lat)
print("지번주소:", jibun)

road = coordinates_to_road_address(lon, lat)
print("도로명주소:", road)

# Step 3. 지번주소 → 자치구, 행정동명
gu, dong = extract_gu_and_dong(jibun)
print("자치구:", gu, "행정동:", dong)

# Step 4. 코드 매핑
gu_code, dong_code, admin_dong = get_gu_dong_codes(gu, dong)
print("자치구코드:", gu_code, "행정동코드:", dong_code, "행정동명:", admin_dong)
