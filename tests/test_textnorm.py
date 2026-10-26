"""Chạy: python -m pytest tests -q   (hoặc: python tests/test_textnorm.py)"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from provinces import OLD_PROVINCES, PROVINCE_BY_CODE, PROVINCES, PROVINCE_CODES  # noqa: E402
from textnorm import (clean_channel_name, clean_title, clean_video_title, find_province_mentions,  # noqa: E402
                      genre_bucket, genre_hint_from_playlist, match_provinces, parse_count, split_artists,
                      track_key)

# 63 mã tỉnh cũ mà Google Trends trả về (lấy từ response thật ngày 27/09/2026)
GOOGLE_TRENDS_ISO = """VN-22 VN-HN VN-DN VN-SG VN-41 VN-01 VN-07 VN-09 VN-13 VN-14 VN-18 VN-20 VN-21 VN-05 VN-23 VN-24
VN-25 VN-26 VN-27 VN-28 VN-29 VN-30 VN-31 VN-32 VN-33 VN-34 VN-35 VN-36 VN-37 VN-39 VN-40 VN-04 VN-43 VN-44 VN-45 VN-46
VN-47 VN-49 VN-50 VN-51 VN-52 VN-53 VN-54 VN-55 VN-56 VN-57 VN-58 VN-59 VN-61 VN-63 VN-66 VN-67 VN-68 VN-69 VN-70 VN-71
VN-72 VN-73 VN-CT VN-03 VN-02 VN-HP VN-06""".split()


def test_34_provinces():
    assert len(PROVINCES) == 34
    assert len(set(PROVINCE_CODES)) == 34


def test_old_provinces_cover_google_trends():
    assert len(OLD_PROVINCES) == 63
    assert {o[0] for o in OLD_PROVINCES} == set(GOOGLE_TRENDS_ISO)
    assert all(o[2] in PROVINCE_BY_CODE for o in OLD_PROVINCES)
    assert {o[2] for o in OLD_PROVINCES} == set(PROVINCE_CODES)       # tỉnh mới nào cũng có tỉnh cũ
    for p in PROVINCES:                                                # dân số tỉnh mới = tổng tỉnh cũ
        assert abs(sum(o[3] for o in OLD_PROVINCES if o[2] == p["code"]) - p["population"]) < 0.02, p["code"]


def test_track_key_cross_platform():
    # Spotify embed / Apple RSS / YouTube chart cùng 1 bài
    k1 = track_key("Tìm Em (feat. Bảo Anh)", "Hngle,\xa0Bảo Anh")
    k2 = track_key("Tìm Em (feat. Bảo Anh)", "Hngle")
    k3 = track_key("TÌM EM", ["Hngle", "Bảo Anh"])
    assert k1 == k2 == k3 == "tim-em__hngle"
    assert track_key("LƯU NIÊN", ["Jack - J97"]) == "luu-nien__jack-j97"
    assert track_key("Bơ Vơ", "Hoài Lâm") == "bo-vo__hoai-lam"
    # remix là bài khác
    assert track_key("Tìm Em (Remix)", "Hngle") != k1


def test_clean_title_and_artists():
    assert clean_title("Nơi Này Có Anh | Official Music Video") == "Nơi Này Có Anh"
    assert clean_title("Hãy Trao Cho Anh (Official MV)") == "Hãy Trao Cho Anh"
    assert clean_title("See Tình ft. Binz") == "See Tình"
    assert split_artists("Sơn Tùng M-TP, Snoop Dogg & Madison Beer") == ["Sơn Tùng M-TP", "Snoop Dogg", "Madison Beer"]
    assert split_artists("Jack - J97") == ["Jack - J97"]


def test_youtube_video_titles():
    # các tên video thật lấy từ BXH YouTube Việt Nam
    assert clean_video_title("PHƯƠNG MỸ CHI x DTAP | 'THIÊN ĐƯỜNG VỚI NGƯỜI THƯƠNG' | OFFICIAL MUSIC VIDEO",
                             ["Phương Mỹ Chi"]) == "THIÊN ĐƯỜNG VỚI NGƯỜI THƯƠNG"
    assert clean_video_title("GIÁ NHƯ ANH LÀ EM - LỆ QUYÊN | OFFICIAL MUSIC VIDEO", ["Lệ Quyên"]) == "GIÁ NHƯ ANH LÀ EM"
    assert clean_video_title("JACK - J97 | TAM THÁI TỬ | Album TAM THÁI TỬ - Track No.11", ["Jack - J97"]) == "TAM THÁI TỬ"
    assert clean_video_title("Ác Mộng Đẹp - Đạt G | Chapter 3: Đẹp | Album \"Ác Mộng Đẹp\"", ["DatG Music"]) == "Ác Mộng Đẹp"
    assert clean_video_title("MAIQUINN - Một Người Như Thế (ft. KIMLONG) | OFFICIAL VISUALIZED BEAUTY",
                             ["MAIQUINN"]) == "Một Người Như Thế"
    assert clean_video_title("Trăm Năm Chỉ Một Người – Một Đời Chỉ Yêu Một Người | Hoàng K Official",
                             ["Hoàng K Official"]) == "Trăm Năm Chỉ Một Người – Một Đời Chỉ Yêu Một Người"
    assert clean_video_title("LƯU NIÊN", ["Jack - J97"]) == "LƯU NIÊN"
    assert clean_video_title("LƯU NIÊN - NGUYỄN ĐÌNH VŨ | JACK J97 | TAM THÁI TỬ | COVER",
                             ["Nguyễn Đình Vũ"]) == "LƯU NIÊN"
    assert clean_channel_name("DatG Music") == "DatG"
    assert clean_channel_name("Hoàng K Official") == "Hoàng K"


def test_match_provinces_accented():
    assert match_provinces("Ai nghe từ Nghệ An điểm danh nào") == ["nghe-an"]
    assert set(match_provinces("Trà Vinh với An Giang có mặt!!")) == {"vinh-long", "an-giang"}
    assert match_provinces("Sài Gòn mưa quá nghe bài này chill ghê") == ["ho-chi-minh"]
    assert match_provinces("Thanh Hoá quê mình") == ["thanh-hoa"]  # dấu đặt kiểu cũ
    assert match_provinces("Mình ở Bình Dương nè") == ["ho-chi-minh"]  # tỉnh cũ -> tỉnh mới
    assert match_provinces("hà nội mùa thu") == ["ha-noi"]


def test_match_provinces_no_false_positive():
    assert match_provinces("nghe an yên quá") == []        # 'nghe an' không dấu dễ nhầm
    assert match_provinces("Nghe an nhiên lắm") == []
    assert match_provinces("bài này đa năng thật") == []    # đa năng != Đà Nẵng
    assert match_provinces("Bác Hồ Chí Minh vĩ đại") == []  # tên Bác, không phải TP
    assert match_provinces("hai phòng karaoke") == []       # 'hai phòng' = 2 phòng
    assert match_provinces("ở lòng an nhiên") == []


def test_mention_kinds():
    assert find_province_mentions("Ai ở Nghệ An điểm danh nào") == [("nghe-an", "self")]
    assert find_province_mentions("quê mình Trà Vinh nè") == [("vinh-long", "self")]
    assert find_province_mentions("Sài Gòn đâu rồi") == [("ho-chi-minh", "self")]
    assert find_province_mentions("dân Hải Phòng có ai không") == [("hai-phong", "self")]
    assert find_province_mentions("minh o can tho") == [("can-tho", "self")]
    assert find_province_mentions("Hà Nội mùa thu đẹp quá") == [("ha-noi", "mention")]
    assert find_province_mentions("PMC đc tỉnh Trà Vinh ghi nhận") == [("vinh-long", "mention")]
    # các câu thật từ bình luận YouTube
    assert find_province_mentions("làm mình nhớ quê quá quê mình càng long trà vinh") == [("vinh-long", "self")]
    assert find_province_mentions("co an giang luon") == [("an-giang", "self")]
    assert find_province_mentions("thật ấy chớ dân Thanh Hóa chúng mình nhảy cảm thật") == [("thanh-hoa", "self")]
    assert find_province_mentions("anh ở thanh hóa à, chỗ a có nhiều rau má") == [("thanh-hoa", "mention")]
    assert find_province_mentions("Bạn này hát ở Hà Tĩnh thấy hát hay quá") == [("ha-tinh", "mention")]
    assert find_province_mentions("giống hồ đá ở đồng nai quá") == [("dong-nai", "mention")]
    assert find_province_mentions("Hà Nội luôn đẹp") == [("ha-noi", "mention")]


def test_mention_filters():
    # bài hát VỀ Sài Gòn -> nhắc Sài Gòn không nói lên người nghe ở đâu
    assert find_province_mentions("Nghe bài này nhớ Sài Gòn ghê", title="Sài Gòn Đau Lòng Quá") == []
    # credit / nơi quay MV
    assert find_province_mentions("Credit: Director X, location quay tại Đà Lạt") == []
    # dán lời bài hát (nhiều dòng)
    lyrics = chr(10).join(["Hà Nội mùa thu", "cây cơm nguội vàng", "cây bàng lá đỏ", "nằm kề bên nhau", "phố xưa nhà cổ"])
    assert find_province_mentions(lyrics) == []


def test_match_provinces_unaccented():
    assert match_provinces("ai o can tho ko") == ["can-tho"]
    assert match_provinces("sg dem nay mua") == ["ho-chi-minh"]
    assert match_provinces("minh o ha noi") == ["ha-noi"]
    assert match_provinces("minh o da lat") == ["lam-dong"]
    assert match_provinces("nghe an yen") == []


def test_genre_bucket():
    assert genre_bucket(["Việt Nam", "V-Pop"]) == "V-Pop"
    assert genre_bucket(["Việt Nam", "Rap Việt"]) == "Rap Việt"
    assert genre_bucket(["Nhạc Trữ Tình"]) == "Nhạc Trữ Tình"
    assert genre_bucket(["Pop"], title="Patient Zero", artists="Taylor Swift") == "US-UK"
    assert genre_bucket(["Hip-Hop/Rap"], title="Tháp Drill Tự Do", artists="MCK") == "Rap Việt"
    assert genre_bucket([], title="Bơ Vơ", artists="Hoài Lâm") == "V-Pop"
    assert genre_bucket(["K-Pop"]) == "K-Pop/Châu Á"
    # Zing gắn LAVIEM vào playlist 'Nhạc Nhật Bản' nhưng nghệ sĩ Việt -> không phải nhạc ngoại
    assert genre_bucket(["Nhật Bản"], title="LAVIEM", artists=['TINH HÀ "SAY HI"', "Quang Hùng MasterD"]) == "V-Pop"
    assert genre_hint_from_playlist("Top 100 Nhạc Trữ Tình Hay Nhất") == "Nhạc Trữ Tình"
    assert genre_hint_from_playlist("Top 100 Bài Hát Nhạc Trẻ Hay Nhất") == "Nhạc Trẻ"


def test_parse_count():
    assert parse_count("1,2K") == 1200
    assert parse_count("3.4M") == 3400000
    assert parse_count("12 N") == 12000
    assert parse_count("1.234") == 1234
    assert parse_count(None) == 0


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
