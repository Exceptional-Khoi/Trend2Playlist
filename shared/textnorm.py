"""Chuẩn hoá văn bản dùng chung cho crawler, Spark job và API.

- track_key(): khoá định danh 1 bài hát xuyên nền tảng (entity resolution đơn giản)
- match_provinces(): tìm các tỉnh được nhắc tới trong bình luận
- genre_bucket(): gom thể loại của nhiều nền tảng về ~10 nhóm chung

Chỉ dùng thư viện chuẩn để chạy được trong Spark executor mà không cần cài thêm.
"""
import re
import unicodedata

try:  # chạy trong package (tests) hoặc file phẳng (ConfigMap trong k8s)
    from .provinces import PROVINCES, AMBIGUOUS_WITHOUT_DIACRITICS
except ImportError:  # pragma: no cover
    from provinces import PROVINCES, AMBIGUOUS_WITHOUT_DIACRITICS


# ----------------------------------------------------------------- cơ bản
def strip_accents(s):
    """'Sơn Tùng M-TP' -> 'Son Tung M-TP' (giữ nguyên hoa/thường)."""
    if not s:
        return ""
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def norm_text(s):
    """Chữ thường, bỏ dấu, chỉ giữ chữ-số, gộp khoảng trắng."""
    s = strip_accents(s or "").lower()
    s = re.sub(r"[^0-9a-z]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def slug(s):
    return norm_text(s).replace(" ", "-")


def has_diacritics(s):
    """True nếu chuỗi có dấu tiếng Việt (người gõ có dấu)."""
    s = s or ""
    return strip_accents(s) != s


# ----------------------------------------------------------------- bài hát
_BRACKET_NOISE = re.compile(
    r"\s*[\(\[【][^\)\]】]*?\b(feat|ft|with|w/|prod|official|mv|m/v|lyric|lyrics|audio|video|visuali[sz]er|"
    r"teaser|performance|vietsub|karaoke|4k|hd|from|ost|soundtrack|nhạc phim|theme song)\b[^\)\]】]*[\)\]】]",
    re.I,
)
_FEAT_TAIL = re.compile(r"\s+(?:feat\.?|ft\.?|featuring)\s+.*$", re.I)
_PIPE_TAIL = re.compile(r"\s*[|｜]\s*.*$")
_ARTIST_SPLIT = re.compile(r"\s*(?:,|&|;|/|×|\+|\bx\b|\bft\.?(?=\s)|\bfeat\.?(?=\s)|\bfeaturing\b|\bwith\b|\bvà\b)\s*", re.I)


def clean_title(title):
    """Bỏ phần '(feat. ...)', '(Official MV)', '| Lyrics'... khỏi tên bài."""
    t = (title or "").strip()
    t = _BRACKET_NOISE.sub(" ", t)
    t = _FEAT_TAIL.sub("", t)
    t = _PIPE_TAIL.sub("", t)
    return re.sub(r"\s+", " ", t).strip() or (title or "").strip()


def split_artists(artists):
    """Nhận str hoặc list, trả về list tên nghệ sĩ đã tách."""
    if artists is None:
        return []
    if isinstance(artists, (list, tuple)):
        out = []
        for a in artists:
            out.extend(split_artists(a))
        return out
    parts = _ARTIST_SPLIT.split(str(artists).replace("\xa0", " "))
    return [p.strip() for p in parts if p and p.strip()]


_VIDEO_NOISE = re.compile(r"\b(official|music video|mv|m/v|lyric|lyrics|audio|visuali[sz]ed?r?|album|track no|chapter|"
                          r"teaser|trailer|live stage|performance|vietsub|karaoke|4k)\b", re.I)
_CHANNEL_SUFFIX = re.compile(r"\s*(-\s*topic|official|music|entertainment|channel|records|media|vevo|tv)\s*$", re.I)


def clean_channel_name(name):
    """'DatG Music' -> 'DatG', 'Hoàng K Official' -> 'Hoàng K' (tên kênh YouTube -> tên nghệ sĩ)."""
    n = (name or "").strip()
    for _ in range(2):
        n = _CHANNEL_SUFFIX.sub("", n).strip()
    return n or (name or "").strip()


def _compact(s):
    return norm_text(s).replace(" ", "")


def _is_artist(segment, artists):
    seg = _compact(segment)
    if len(seg) < 2:
        return False
    for a in split_artists(artists):
        ca = _compact(clean_channel_name(a))
        if len(ca) >= 3 and (ca in seg or seg in ca):
            return True
    return False


def clean_video_title(title, artists):
    """Tên video YouTube -> tên bài.
    'PHƯƠNG MỸ CHI x DTAP | 'THIÊN ĐƯỜNG...' | OFFICIAL MV' -> 'THIÊN ĐƯỜNG...'
    'GIÁ NHƯ ANH LÀ EM - LỆ QUYÊN | OFFICIAL MUSIC VIDEO'   -> 'GIÁ NHƯ ANH LÀ EM'
    """
    segs = [x.strip(" '\"“”‘’") for x in re.split(r"[|｜]", title or "") if x.strip(" '\"“”‘’")]
    keep = [x for x in segs if not _VIDEO_NOISE.search(x)] or segs[:1]
    cand = [x for x in keep if not _is_artist(x, artists)] or keep
    t = cand[0] if cand else (title or "")
    parts = re.split(r"\s+[-–—]\s+", t, maxsplit=1)
    if len(parts) == 2:
        left, right = parts
        if _is_artist(left, artists) and not _is_artist(right, artists):
            t = right
        elif _is_artist(right, artists) and not _is_artist(left, artists):
            t = left
    return clean_title(t.strip(" '\"“”‘’"))


def track_key(title, artists):
    """Khoá bài hát: '<tên bài>__<nghệ sĩ chính>' dạng slug, ví dụ 'tim-em__hngle'."""
    arts = split_artists(artists)
    primary = arts[0] if arts else "unknown"
    return f"{slug(clean_title(title)) or 'unknown'}__{slug(primary) or 'unknown'}"


def parse_count(s):
    """'1,2K' / '3.4M' / '12 N' / '1.234' -> int. Dùng cho số like dạng chữ."""
    if s is None:
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    t = str(s).strip().replace("\xa0", " ").upper()
    m = re.match(r"^([\d.,]+)\s*([KMB]|N|TR|T)?", t)
    if not m:
        return 0
    num, unit = m.group(1), m.group(2)
    if unit:
        num = float(num.replace(",", "."))
        mult = {"K": 1e3, "N": 1e3, "M": 1e6, "TR": 1e6, "B": 1e9, "T": 1e9}[unit]
        return int(num * mult)
    return int(re.sub(r"[.,]", "", num) or 0)


# ----------------------------------------------------------------- thể loại
GENRE_BUCKETS = ["V-Pop", "Rap Việt", "Nhạc Trữ Tình", "EDM/Remix", "Indie/Rock Việt", "Nhạc Trịnh/Tiền Chiến",
                 "Quê Hương/Cải Lương", "US-UK", "K-Pop/Châu Á", "Khác"]


def genre_bucket(genres, title="", artists=None):
    """Gom danh sách thể loại (Zing/Apple/playlist) thành 1 nhóm chung."""
    gs = [norm_text(g) for g in (genres or []) if g]
    joined = " | ".join(gs)
    text_vn = has_diacritics(title) or any(has_diacritics(a) for a in split_artists(artists))
    is_vn = text_vn or any(k in joined for k in ("viet", "v pop", "nhac tre"))

    def has(*keys):
        return any(k in joined for k in keys)

    if has("trinh", "tien chien"):
        return "Nhạc Trịnh/Tiền Chiến"
    if has("tru tinh", "bolero", "nhac vang"):
        return "Nhạc Trữ Tình"
    if has("cai luong", "que huong", "dan ca", "cach mang"):
        return "Quê Hương/Cải Lương"
    # tên bài/nghệ sĩ có dấu tiếng Việt -> không xếp vào nhạc ngoại dù playlist gắn nhãn ngoại
    if has("au my", "us uk") and not has("viet") and not text_vn:
        return "US-UK"
    if has("han quoc", "k pop", "kpop", "nhat ban", "j pop", "hoa ngu", "c pop", "chau a") and not text_vn:
        return "K-Pop/Châu Á"
    if has("rap", "hip hop"):
        return "Rap Việt" if is_vn else "US-UK"
    if has("edm", "remix", "dance", "electronic", "house", "trance"):
        return "EDM/Remix" if is_vn else "US-UK"
    if has("indie", "rock", "alternative"):
        return "Indie/Rock Việt" if is_vn else "US-UK"
    if has("v pop", "nhac tre", "viet nam", "nhac phim", "vietnamese"):
        return "V-Pop"
    if has("pop", "r b", "soul", "singer"):
        return "V-Pop" if is_vn else "US-UK"
    return "V-Pop" if is_vn else "Khác"


def genre_hint_from_playlist(name):
    """'Top 100 Nhạc Trữ Tình Hay Nhất' -> 'Nhạc Trữ Tình'."""
    t = re.sub(r"(?i)^top\s*\d+\s*", "", name or "")
    t = re.sub(r"(?i)\s*(hay nhất|việt nam hay nhất)$", "", t)
    t = re.sub(r"(?i)^bài hát\s+", "", t)
    return t.strip()


# ----------------------------------------------------------------- tỉnh thành
_TONE_FIX = {"oà": "òa", "oá": "óa", "oả": "ỏa", "oã": "õa", "oạ": "ọa", "uỳ": "ùy", "uý": "úy", "uỷ": "ủy", "uỹ": "ũy", "uỵ": "ụy"}


def _fix_tone(s):
    # 'thanh hoá' và 'thanh hóa' cùng nghĩa -> đưa về 1 kiểu bỏ dấu thanh
    for a, b in _TONE_FIX.items():
        s = s.replace(a, b)
    return s


def _light(s):
    """Chữ thường NFC, giữ dấu, thay dấu câu bằng khoảng trắng."""
    s = _fix_tone(unicodedata.normalize("NFC", (s or "").lower()))
    return re.sub(r"\s+", " ", re.sub(r"[^\w]+", " ", s)).strip()


def _build_patterns():
    accented, plain = [], []
    for p in PROVINCES:
        for alias in p["aliases"]:
            a = _light(alias)
            # dùng cho bình luận CÓ dấu: so khớp nguyên văn (giữ dấu) -> 'lòng an' != 'long an'
            accented.append((re.compile(r"(?<!\w)" + re.escape(a) + r"(?!\w)"), p["code"]))
            # dùng cho bình luận KHÔNG dấu: so trên văn bản bỏ dấu, loại alias dễ nhầm
            stripped = norm_text(a)
            if stripped not in AMBIGUOUS_WITHOUT_DIACRITICS:
                plain.append((re.compile(r"(?<![0-9a-z])" + re.escape(stripped) + r"(?![0-9a-z])"), p["code"]))
    return accented, plain


_ACCENTED, _PLAIN = _build_patterns()


def _iter_matches(text):
    """(mã tỉnh, vị trí bắt đầu, vị trí kết thúc, văn bản đã chuẩn hoá) cho mọi lần nhắc tỉnh."""
    if not text:
        return
    light = _light(text)
    if has_diacritics(light):
        patterns, target = _ACCENTED, light
    else:
        patterns, target = _PLAIN, norm_text(light)
    for rx, code in patterns:
        for m in rx.finditer(target):
            yield code, m.start(), m.end(), target


def match_provinces(text):
    """Trả về list mã tỉnh (không trùng) được nhắc trong bình luận.

    - Bình luận có dấu: khớp nguyên văn có dấu (tránh 'nghe an yên' -> Nghệ An, 'lòng an' -> Long An)
    - Bình luận không dấu: khớp dạng bỏ dấu, trừ các alias dễ nhầm (AMBIGUOUS_WITHOUT_DIACRITICS)
    """
    found = []
    for code, _, _, _ in _iter_matches(text):
        if code not in found:
            found.append(code)
    return found


# Dấu hiệu người viết đang nói MÌNH ở đâu (so trên văn bản bỏ dấu, ngay trước tên tỉnh):
#   "quê/người/dân/gái/trai X", "đến từ/gửi từ X"            -> nói về gốc gác
#   "(ai|mình|tôi|tui|em|tớ|mk|t|tao|bọn mình...) (đang)? (ở|tại|từ|sống ở...) X"  -> ngôi thứ nhất (+ "ai" điểm danh)
# KHÔNG tính "ở X" trơn: "bạn này hát ở Hà Tĩnh", "giống hồ đá ở Đồng Nai", "anh ở Thanh Hóa à" (hỏi người khác)
_SELF_BEFORE = re.compile(
    r"(?:^|\s)(?:que|que huong|nguoi|dan|gai|trai|den tu|gui tu|"
    r"(?:ai|minh|toi|tui|em|to|mk|t|tao|bon minh|chung minh|nha minh|nha em)"
    r"(?: dang| hien| cung| van)?(?: (?:o|tai|tu|song o|song tai|lam o|hoc o|den tu|gui tu))?)$")
_QUE_NEAR = re.compile(r"(?:^|\s)que(?: huong)?(?:\s+\S+){0,3}$")      # "quê mình Càng Long Trà Vinh"
# Dấu hiệu ngay SAU tên tỉnh: "Nghệ An điểm danh", "Cần Thơ đâu rồi", "Huế có ai không", "Sài Gòn nè", "có An Giang luôn"
_SELF_AFTER = re.compile(r"^(?:diem danh|dau|ne|nhe|day|co ai|que toi|que minh|que em|que tui|la que|cua toi|cua minh|"
                         r"minh day|here)(?:\s|$)|^(?:luon|nua)\s*$")
# Bình luận kiểu credit / lời bài hát: tên tỉnh thường là nơi quay MV hoặc nằm trong lời, không phải người nghe
_CREDIT = re.compile(r"\b(credit|credits|director|producer|executive|production|stylist|makeup|make up|choreograph\w*|"
                     r"dao dien|san xuat|bien dao|phoi khi|hoa am|quay tai|location|mixing|mastering|composer|"
                     r"sang tac|art director|dop)\b")
MENTION_WEIGHT = {"self": 1.0, "mention": 0.3}


def is_credit_or_lyrics(text):
    t = text or ""
    return t.count("\n") >= 4 or bool(_CREDIT.search(norm_text(t)))


def find_province_mentions(text, title=""):
    """Nhận diện tỉnh trong bình luận kèm loại:
      'self'    : người viết tự nhận mình ở/đến từ tỉnh đó ("ai ở Nghệ An điểm danh") -> trọng số 1.0
      'mention' : chỉ nhắc tên ("Hà Nội mùa thu đẹp quá")                                 -> trọng số 0.3
    Bỏ qua: bình luận dạng credit/lời bài hát; tỉnh có trong chính tên bài (bài hát VỀ tỉnh đó).
    Trả về list (mã tỉnh, loại)."""
    if not text or is_credit_or_lyrics(text):
        return []
    in_title = set(match_provinces(title)) if title else set()
    kinds = {}
    for code, start, end, target in _iter_matches(text):
        if code in in_title:
            continue
        before = norm_text(target[max(0, start - 30):start])
        after = norm_text(target[end:end + 30])
        kind = "self" if (_SELF_BEFORE.search(before) or _QUE_NEAR.search(before) or _SELF_AFTER.search(after)) else "mention"
        if kinds.get(code) != "self":
            kinds[code] = kind
    return list(kinds.items())
