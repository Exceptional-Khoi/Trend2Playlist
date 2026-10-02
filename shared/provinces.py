"""34 tỉnh/thành Việt Nam sau sáp nhập (NQ 202/2025/QH15, hiệu lực 01/07/2025).

Mỗi tỉnh gồm:
  code        : mã slug dùng xuyên suốt hệ thống (Kafka key, ES doc id, ...)
  name        : tên hiển thị
  region      : Bắc / Trung / Nam
  subregion   : vùng kinh tế - xã hội
  lat, lon    : toạ độ xấp xỉ (tâm vùng) để vẽ bản đồ
  population  : dân số xấp xỉ (triệu người) = tổng các tỉnh cũ (GSO 2023),
                chỉ dùng làm TRỌNG SỐ cho bộ giả lập người nghe
  aliases     : cách người dùng hay gọi tỉnh trong bình luận (gồm cả tên tỉnh cũ
                trước sáp nhập, thành phố lớn, biệt danh). Viết có dấu.
"""

PROVINCES = [
    # ---------------- Miền Bắc ----------------
    dict(code="ha-noi", name="Hà Nội", region="Bắc", subregion="Đồng bằng sông Hồng", lat=21.03, lon=105.85, population=8.59,
         aliases=["hà nội", "hanoi", "hà thành"]),
    dict(code="hai-phong", name="Hải Phòng", region="Bắc", subregion="Đồng bằng sông Hồng", lat=20.88, lon=106.50, population=4.05,
         aliases=["hải phòng", "đất cảng", "hải dương"]),
    dict(code="quang-ninh", name="Quảng Ninh", region="Bắc", subregion="Đồng bằng sông Hồng", lat=21.10, lon=107.30, population=1.38,
         aliases=["quảng ninh", "hạ long", "móng cái", "cẩm phả", "uông bí"]),
    dict(code="bac-ninh", name="Bắc Ninh", region="Bắc", subregion="Đồng bằng sông Hồng", lat=21.27, lon=106.20, population=3.41,
         aliases=["bắc ninh", "kinh bắc", "bắc giang"]),
    dict(code="hung-yen", name="Hưng Yên", region="Bắc", subregion="Đồng bằng sông Hồng", lat=20.60, lon=106.20, population=3.17,
         aliases=["hưng yên", "thái bình"]),
    dict(code="ninh-binh", name="Ninh Bình", region="Bắc", subregion="Đồng bằng sông Hồng", lat=20.40, lon=106.02, population=3.77,
         aliases=["ninh bình", "nam định", "hà nam", "phủ lý"]),
    dict(code="phu-tho", name="Phú Thọ", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=21.15, lon=105.35, population=3.59,
         aliases=["phú thọ", "việt trì", "vĩnh phúc", "vĩnh yên", "tỉnh hòa bình", "tỉnh hoà bình"]),
    dict(code="thai-nguyen", name="Thái Nguyên", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=21.85, lon=105.85, population=1.67,
         aliases=["thái nguyên", "bắc kạn", "bắc cạn"]),
    dict(code="tuyen-quang", name="Tuyên Quang", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=22.30, lon=105.10, population=1.70,
         aliases=["tuyên quang", "hà giang"]),
    dict(code="lao-cai", name="Lào Cai", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=22.10, lon=104.40, population=1.62,
         aliases=["lào cai", "sa pa", "sapa", "yên bái"]),
    dict(code="lai-chau", name="Lai Châu", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=22.40, lon=103.30, population=0.48,
         aliases=["lai châu"]),
    dict(code="dien-bien", name="Điện Biên", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=21.60, lon=103.00, population=0.64,
         aliases=["điện biên"]),
    dict(code="son-la", name="Sơn La", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=21.20, lon=104.00, population=1.29,
         aliases=["sơn la", "mộc châu"]),
    dict(code="cao-bang", name="Cao Bằng", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=22.67, lon=106.26, population=0.54,
         aliases=["cao bằng"]),
    dict(code="lang-son", name="Lạng Sơn", region="Bắc", subregion="Trung du & miền núi phía Bắc", lat=21.85, lon=106.76, population=0.80,
         aliases=["lạng sơn", "xứ lạng"]),
    # ---------------- Miền Trung & Tây Nguyên ----------------
    dict(code="thanh-hoa", name="Thanh Hóa", region="Trung", subregion="Bắc Trung Bộ", lat=19.95, lon=105.45, population=3.72,
         aliases=["thanh hóa", "thanh hoá", "xứ thanh"]),
    dict(code="nghe-an", name="Nghệ An", region="Trung", subregion="Bắc Trung Bộ", lat=19.20, lon=104.90, population=3.44,
         aliases=["nghệ an", "xứ nghệ"]),
    dict(code="ha-tinh", name="Hà Tĩnh", region="Trung", subregion="Bắc Trung Bộ", lat=18.30, lon=105.90, population=1.32,
         aliases=["hà tĩnh"]),
    dict(code="quang-tri", name="Quảng Trị", region="Trung", subregion="Bắc Trung Bộ", lat=17.20, lon=106.60, population=1.56,
         aliases=["quảng trị", "quảng bình", "đồng hới"]),
    dict(code="hue", name="Huế", region="Trung", subregion="Bắc Trung Bộ", lat=16.40, lon=107.60, population=1.16,
         aliases=["huế", "cố đô huế", "thừa thiên huế"]),
    dict(code="da-nang", name="Đà Nẵng", region="Trung", subregion="Duyên hải Nam Trung Bộ", lat=15.80, lon=108.05, population=2.74,
         aliases=["đà nẵng", "quảng nam", "hội an", "tam kỳ"]),
    dict(code="quang-ngai", name="Quảng Ngãi", region="Trung", subregion="Duyên hải Nam Trung Bộ", lat=14.75, lon=108.30, population=1.83,
         aliases=["quảng ngãi", "kon tum"]),
    dict(code="gia-lai", name="Gia Lai", region="Trung", subregion="Tây Nguyên", lat=13.95, lon=108.55, population=3.09,
         aliases=["gia lai", "pleiku", "plei ku", "bình định", "quy nhơn", "đất võ"]),
    dict(code="khanh-hoa", name="Khánh Hòa", region="Trung", subregion="Duyên hải Nam Trung Bộ", lat=11.95, lon=109.05, population=1.86,
         aliases=["khánh hòa", "khánh hoà", "nha trang", "cam ranh", "ninh thuận", "phan rang"]),
    dict(code="dak-lak", name="Đắk Lắk", region="Trung", subregion="Tây Nguyên", lat=12.85, lon=108.55, population=2.80,
         aliases=["đắk lắk", "đắc lắc", "đăk lăk", "daklak", "buôn ma thuột", "buôn mê thuột", "bmt", "phú yên", "tuy hòa", "tuy hoà"]),
    dict(code="lam-dong", name="Lâm Đồng", region="Trung", subregion="Tây Nguyên", lat=11.65, lon=108.05, population=3.26,
         aliases=["lâm đồng", "đà lạt", "bảo lộc", "đắk nông", "đắc nông", "bình thuận", "phan thiết", "mũi né"]),
    # ---------------- Miền Nam ----------------
    dict(code="ho-chi-minh", name="TP. Hồ Chí Minh", region="Nam", subregion="Đông Nam Bộ", lat=10.85, lon=106.75, population=13.41,
         # KHÔNG dùng "hồ chí minh" đơn lẻ vì trùng tên Bác Hồ trong bình luận nhạc cách mạng
         aliases=["sài gòn", "saigon", "sài thành", "tp hcm", "tp.hcm", "tphcm", "hcm", "hcmc", "sg",
                  "thành phố hồ chí minh", "tp hồ chí minh", "bình dương", "thủ dầu một", "vũng tàu", "bà rịa",
                  "brvt", "côn đảo", "thủ đức", "củ chi"]),
    dict(code="dong-nai", name="Đồng Nai", region="Nam", subregion="Đông Nam Bộ", lat=11.35, lon=106.95, population=4.29,
         aliases=["đồng nai", "biên hòa", "biên hoà", "bình phước"]),
    dict(code="tay-ninh", name="Tây Ninh", region="Nam", subregion="Đông Nam Bộ", lat=10.95, lon=106.25, population=2.92,
         aliases=["tây ninh", "long an", "tân an"]),
    dict(code="dong-thap", name="Đồng Tháp", region="Nam", subregion="Đồng bằng sông Cửu Long", lat=10.45, lon=106.00, population=3.39,
         aliases=["đồng tháp", "cao lãnh", "sa đéc", "tiền giang", "mỹ tho"]),
    dict(code="vinh-long", name="Vĩnh Long", region="Nam", subregion="Đồng bằng sông Cửu Long", lat=10.10, lon=106.25, population=3.35,
         aliases=["vĩnh long", "bến tre", "trà vinh"]),
    dict(code="can-tho", name="Cần Thơ", region="Nam", subregion="Đồng bằng sông Cửu Long", lat=9.80, lon=105.75, population=3.19,
         aliases=["cần thơ", "sóc trăng", "hậu giang", "vị thanh"]),
    dict(code="an-giang", name="An Giang", region="Nam", subregion="Đồng bằng sông Cửu Long", lat=10.25, lon=105.10, population=3.66,
         aliases=["an giang", "long xuyên", "châu đốc", "kiên giang", "rạch giá", "phú quốc", "hà tiên"]),
    dict(code="ca-mau", name="Cà Mau", region="Nam", subregion="Đồng bằng sông Cửu Long", lat=9.20, lon=105.40, population=2.13,
         aliases=["cà mau", "bạc liêu"]),
]

PROVINCE_BY_CODE = {p["code"]: p for p in PROVINCES}
PROVINCE_CODES = [p["code"] for p in PROVINCES]

# 63 tỉnh CŨ (trước 01/07/2025) -> tỉnh mới. Google Trends vẫn báo số liệu theo 63 tỉnh cũ với mã ISO 3166-2
# (VN-SG, VN-57...), nên cần bảng này để gộp về 34 tỉnh mới, trọng số = dân số tỉnh cũ (triệu người, GSO 2023).
OLD_PROVINCES = [
    # (mã ISO, tên tỉnh cũ, mã tỉnh mới, dân số)
    ("VN-HN", "Hà Nội", "ha-noi", 8.59), ("VN-SG", "Hồ Chí Minh", "ho-chi-minh", 9.46),
    ("VN-57", "Bình Dương", "ho-chi-minh", 2.76), ("VN-43", "Bà Rịa - Vũng Tàu", "ho-chi-minh", 1.19),
    ("VN-HP", "Hải Phòng", "hai-phong", 2.10), ("VN-61", "Hải Dương", "hai-phong", 1.95),
    ("VN-13", "Quảng Ninh", "quang-ninh", 1.38),
    ("VN-56", "Bắc Ninh", "bac-ninh", 1.49), ("VN-54", "Bắc Giang", "bac-ninh", 1.92),
    ("VN-66", "Hưng Yên", "hung-yen", 1.29), ("VN-20", "Thái Bình", "hung-yen", 1.88),
    ("VN-18", "Ninh Bình", "ninh-binh", 1.01), ("VN-63", "Hà Nam", "ninh-binh", 0.88), ("VN-67", "Nam Định", "ninh-binh", 1.88),
    ("VN-68", "Phú Thọ", "phu-tho", 1.51), ("VN-70", "Vĩnh Phúc", "phu-tho", 1.20), ("VN-14", "Hòa Bình", "phu-tho", 0.88),
    ("VN-69", "Thái Nguyên", "thai-nguyen", 1.34), ("VN-53", "Bắc Kạn", "thai-nguyen", 0.33),
    ("VN-07", "Tuyên Quang", "tuyen-quang", 0.80), ("VN-03", "Hà Giang", "tuyen-quang", 0.90),
    ("VN-02", "Lào Cai", "lao-cai", 0.77), ("VN-06", "Yên Bái", "lao-cai", 0.85),
    ("VN-01", "Lai Châu", "lai-chau", 0.48), ("VN-71", "Điện Biên", "dien-bien", 0.64),
    ("VN-05", "Sơn La", "son-la", 1.29), ("VN-04", "Cao Bằng", "cao-bang", 0.54), ("VN-09", "Lạng Sơn", "lang-son", 0.80),
    ("VN-21", "Thanh Hóa", "thanh-hoa", 3.72), ("VN-22", "Nghệ An", "nghe-an", 3.44), ("VN-23", "Hà Tĩnh", "ha-tinh", 1.32),
    ("VN-24", "Quảng Bình", "quang-tri", 0.91), ("VN-25", "Quảng Trị", "quang-tri", 0.65),
    ("VN-26", "Thừa Thiên Huế", "hue", 1.16),
    ("VN-DN", "Đà Nẵng", "da-nang", 1.22), ("VN-27", "Quảng Nam", "da-nang", 1.52),
    ("VN-29", "Quảng Ngãi", "quang-ngai", 1.25), ("VN-28", "Kon Tum", "quang-ngai", 0.58),
    ("VN-30", "Gia Lai", "gia-lai", 1.59), ("VN-31", "Bình Định", "gia-lai", 1.50),
    ("VN-34", "Khánh Hòa", "khanh-hoa", 1.26), ("VN-36", "Ninh Thuận", "khanh-hoa", 0.60),
    ("VN-33", "Đắk Lắk", "dak-lak", 1.92), ("VN-32", "Phú Yên", "dak-lak", 0.88),
    ("VN-35", "Lâm Đồng", "lam-dong", 1.33), ("VN-72", "Đắk Nông", "lam-dong", 0.67), ("VN-40", "Bình Thuận", "lam-dong", 1.26),
    ("VN-39", "Đồng Nai", "dong-nai", 3.26), ("VN-58", "Bình Phước", "dong-nai", 1.03),
    ("VN-37", "Tây Ninh", "tay-ninh", 1.19), ("VN-41", "Long An", "tay-ninh", 1.73),
    ("VN-CT", "Cần Thơ", "can-tho", 1.26), ("VN-52", "Sóc Trăng", "can-tho", 1.20), ("VN-73", "Hậu Giang", "can-tho", 0.73),
    ("VN-49", "Vĩnh Long", "vinh-long", 1.03), ("VN-50", "Bến Tre", "vinh-long", 1.30), ("VN-51", "Trà Vinh", "vinh-long", 1.02),
    ("VN-45", "Đồng Tháp", "dong-thap", 1.60), ("VN-46", "Tiền Giang", "dong-thap", 1.79),
    ("VN-59", "Cà Mau", "ca-mau", 1.21), ("VN-55", "Bạc Liêu", "ca-mau", 0.92),
    ("VN-44", "An Giang", "an-giang", 1.91), ("VN-47", "Kiên Giang", "an-giang", 1.75),
]
OLD_BY_ISO = {o[0]: o for o in OLD_PROVINCES}

# Các alias khi BỎ DẤU sẽ trùng với từ thông dụng -> chỉ nhận khi viết có dấu.
#   "nghe an"  <- "nghe an yên"      "da nang"  <- "đa năng"
#   "hai phong"<- "hai phòng"        "gia lai"  <- "già lại"
#   "long an"  <- "lòng an nhiên"    "hue"      <- tên "Huệ"
#   "quang nam", "quang binh" <- tên người "Quang Nam", "Quang Bình"
#   "thai binh", "hoa binh"   <- "thái bình" (yên bình), "hòa bình" (peace)
AMBIGUOUS_WITHOUT_DIACRITICS = {
    "nghe an", "da nang", "hai phong", "gia lai", "long an", "hue", "quang nam", "quang binh",
    "thai binh", "tinh hoa binh", "khanh hoa", "ha nam", "tam ky",
}

# Mức độ ưa thích nhóm thể loại theo vùng - CHỈ dùng cho bộ giả lập người nghe.
# Đây là GIẢ ĐỊNH của nhóm (có thể chỉnh), không phải dữ liệu crawl.
# Khoá là nhóm thể loại chuẩn hoá trong textnorm.GENRE_BUCKETS.
REGION_GENRE_PRIOR = {
    "Bắc":   {"V-Pop": 1.0, "Rap Việt": 0.9, "Indie/Rock Việt": 0.8, "Nhạc Trữ Tình": 0.45, "EDM/Remix": 0.6,
              "Nhạc Trịnh/Tiền Chiến": 0.45, "Quê Hương/Cải Lương": 0.3, "US-UK": 0.6, "K-Pop/Châu Á": 0.55, "Khác": 0.3},
    "Trung": {"V-Pop": 0.9, "Rap Việt": 0.6, "Indie/Rock Việt": 0.5, "Nhạc Trữ Tình": 0.8, "EDM/Remix": 0.6,
              "Nhạc Trịnh/Tiền Chiến": 0.55, "Quê Hương/Cải Lương": 0.8, "US-UK": 0.35, "K-Pop/Châu Á": 0.35, "Khác": 0.3},
    "Nam":   {"V-Pop": 1.0, "Rap Việt": 0.8, "Indie/Rock Việt": 0.55, "Nhạc Trữ Tình": 1.0, "EDM/Remix": 0.85,
              "Nhạc Trịnh/Tiền Chiến": 0.4, "Quê Hương/Cải Lương": 0.75, "US-UK": 0.5, "K-Pop/Châu Á": 0.5, "Khác": 0.3},
}


def province_name(code):
    p = PROVINCE_BY_CODE.get(code)
    return p["name"] if p else code
