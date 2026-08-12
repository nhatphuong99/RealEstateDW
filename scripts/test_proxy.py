import random
import requests

# Link API lấy thẳng list proxy HTTP của Việt Nam (không cần login)
# PROXY_API_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&protocol=http&country=vn"
PROXY_API_URL = "https://fineproxy.org/vi/wp-json/fineproxy/v1/free-proxies/vn/txt"

def fetch_vietnam_proxies():
    """Tự động tải danh sách proxy từ API miễn phí."""
    try:
        response = requests.get(PROXY_API_URL, timeout=10)
        if response.status_code == 200:
            # Tách dòng để lấy danh sách IP:PORT
            proxies = response.text.strip().split("\n")
            # Loại bỏ các dòng trống hoặc bị lỗi khoảng trắng
            return [p.strip() for p in proxies if p.strip()]
    except Exception as e:
        print(f"Lỗi khi tải danh sách proxy: {e}")
    return []


def crawl_with_rotated_proxy(target_url, proxy_list):
    """Thực hiện crawl dữ liệu bằng cách xoay vòng proxy ngẫu nhiên."""
    if not proxy_list:
        print("Không có proxy nào khả dụng!")
        return None

    # Tạo bản sao để không làm ảnh hưởng list gốc khi xóa proxy chết
    available_proxies = proxy_list.copy()

    while available_proxies:
        # Chọn ngẫu nhiên 1 proxy từ danh sách để tránh trùng lặp liên tục
        chosen_proxy = random.choice(available_proxies)
        proxy_config = {
            "http": f"http://{chosen_proxy}",
            "https": f"http://{chosen_proxy}",
        }

        try:
            print(f"Đang thử crawl qua Proxy: {chosen_proxy}...")
            # Kiểm tra và tải trang mục tiêu (Đặt timeout thấp để bỏ qua proxy chậm)
            response = requests.get(target_url, proxies=proxy_config, timeout=5)

            if response.status_code == 200:
                print(f"🎉 Crawl thành công trang bằng proxy: {chosen_proxy}")
                return response.text
        except requests.RequestException:
            # Proxy lỗi hoặc bị nghẽn mạch -> xóa khỏi danh sách hiện tại và thử IP khác
            print(f"❌ Proxy chết hoặc quá chậm: {chosen_proxy}. Đang đổi...")
            available_proxies.remove(chosen_proxy)

    print("Tất cả proxy trong danh sách đều không kết nối được!")
    return None


# === CHẠY THỬ NGHIỆM ===
if __name__ == "__main__":
    # 1. Tự động lấy danh sách IP Việt Nam mới nhất từ API
    vn_proxies = fetch_vietnam_proxies()
    print(f"Tìm thấy {len(vn_proxies)} proxy Việt Nam từ API.")
    print(vn_proxies)

    # 2. Tiến hành crawl thử một trang web bất kỳ
    TARGET = "https://crawler-test.com/titles/title_with_whitespace"
    html_content = crawl_with_rotated_proxy(TARGET, vn_proxies)

    if html_content:
        print("Dữ liệu cào về thành công!")
