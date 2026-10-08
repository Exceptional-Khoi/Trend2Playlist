# Kiểm tra lại khả năng crawl realtime — 07/10/2026

Đã kiểm tra **21 nguồn** từng có dữ liệu chưa rõ nhịp cập nhật, chỉ có mẫu catalog/BXH ngày hoặc chưa crawl được. Lượt này gồm **61 lần thử request**, có endpoint mới và hai lần lấy các nguồn có thể thay đổi. Mỗi lỗi kết nối, HTTP status, phản hồi gốc và thời điểm lấy đều được lưu.

Phát hiện mới rõ nhất là **API bán nhạc công khai của Bandcamp**. **Audiomack và NhacCuaTui có bộ đếm thay đổi trong khoảng 12 phút**. Các kết quả trước ghi “Không với mẫu này” không đủ để kết luận cả nền tảng không có dữ liệu realtime.

## Những thay đổi đã quan sát được

| Nguồn / tín hiệu | Lần đầu | Lần sau | Kết luận có căn cứ |
|---|---|---|---|
| Bandcamp / giao dịch bán | API initial có 70 mục bán; cursor kết thúc `1791383040` | API incremental từ cursor đó trả **385 mục bán mới**, trong đó **304 mục nhạc**, 81 sản phẩm vật lý chưa phân loại | Crawl được tín hiệu mua nhạc gần realtime bằng API công khai |
| Audiomack / bài *Comfort* | 316.775 plays lúc 21:20:02 | 317.242 plays lúc 21:32:10; playlist 2.054 → 2.056 | **+467 plays, +2 mục playlist trong 12 phút 08 giây** |
| NhacCuaTui / bài *LAVIEM* | 57.526 lượt thích lúc 21:20:01 | 57.542 lúc 21:32:10 | **+16 lượt thích trong 12 phút 09 giây**; comment 121 và share 1.036 giữ nguyên |
| SoundCloud / *Flickermood* | 969.932 plays lúc 21:19:59 | 969.932 lúc 21:32:10 | Không đổi trong cửa sổ này; các mẫu trước trong ngày tăng 969.901 → 969.908 → 969.932 |
| NetEase / BXH heat và share | Mẫu 14:51 so với 21:20 | Heat đổi 7/10 vị trí, gồm 4 bài mới; share đổi 8/10 vị trí, gồm 2 bài mới | Đã thấy BXH thay đổi trong ngày; cả hai danh sách giữ nguyên ở lần lấy tiếp theo lúc 21:32 |

Giờ trong bảng là **giờ Việt Nam ngày 07/10/2026**. Thay đổi bộ đếm chứng minh crawler đọc được dữ liệu thay đổi; chưa đo được độ trễ từ lượt nghe/lượt thích thật tới lúc nguồn công bố.

Bandcamp trả thời điểm giao dịch. Ở API initial lần sau, giao dịch mới nhất cách `server_time` **11,36 giây**. JavaScript công khai của trang gọi API mỗi **32 giây** và đệm **120 giây trước khi hiển thị**. Đệm hiển thị không phải cam kết mọi phản hồi API trễ đúng 120 giây. Feed có cả nhạc số, đĩa/merch; parser giữ riêng loại sản phẩm. Số 385 là **mục hàng bán**, không phải 385 bài khác nhau hay 385 lượt nghe.

## Kết quả đầy đủ từng nguồn

“Chưa xác minh” nghĩa là các cách đã thử chưa đủ bằng chứng về realtime. “Cần quyền” ghi rõ dữ liệu nào cần quyền và không hứa token sẽ cung cấp thống kê toàn nền tảng.

| Nguồn | API hoặc cách crawl đã thử thêm | Kết quả thực tế / dữ liệu | Khả năng dùng làm đầu vào cập nhật nhanh |
|---|---|---|---|
| **Bandcamp** | `/artists`; API `/api/salesfeed/1/get_initial`; API `/get?start_date=…` | HTTP 200, có giao dịch mới và thời điểm bán; API không cần token | **Đã lấy được gần realtime — giao dịch mua nhạc** |
| **Audiomack** | Dữ liệu nhúng trong trang bài riêng, lấy hai lần | Plays và playlist tăng trong khoảng 12 phút; likes/repost/comment giữ nguyên giữa hai lần | **Theo dõi được bộ đếm thay đổi**; độ trễ sự kiện chưa xác minh |
| **NhacCuaTui** | Dữ liệu Nuxt trong trang bài riêng, lấy hai lần | Lấy được like/comment/share; +16 like trong khoảng 12 phút | **Theo dõi được tương tác thay đổi**; độ trễ chưa xác minh |
| **SoundCloud** | HTML có counter và bình luận; API comments | Lấy được counter + 26 bình luận có nội dung/thời gian; API comments trả 401; comment mới nhất trong HTML là 23/08 | **Có dữ liệu công khai thay đổi trong ngày**; chưa thấy bình luận mới trong cửa sổ đo, API cần OAuth |
| **NetEase Cloud Music** | Hai trang BXH `8246775932` và `18176153161`, đọc dữ liệu nhúng | 10 bài mỗi BXH; nguồn gọi là realtime; đã thấy đổi bài/hạng giữa các mẫu trong ngày | **Nguồn công bố realtime, đã crawl được**; chưa xác minh nhịp phút/giờ |
| **Spotify** | Official currently-playing và recently-played API | Cả hai trả 401 khi không có token | **Realtime cá nhân có API, cần OAuth**; BXH công khai đã lấy vẫn ngày/tuần |
| **Apple Music** | Official recent-played API và catalog-chart API | Personal endpoint timeout ở lượt mới, từng trả 401; catalog endpoint mới trả 401 | **Cần developer token**, dữ liệu cá nhân còn cần Music User Token; nhịp RSS chart công khai vẫn chưa xác minh |
| **Deezer** | Đúng album/artist comments; album fans và artist `nb_fan`; chart API | Các lần mới bị từ chối kết nối `10061`, kể cả thử lại. Route `track/comments` ở lượt trước là route sai | **Chưa xác minh vì lỗi kết nối**; chưa kết luận nền tảng thiếu realtime |
| **Nhac.vn** | Metadata JSON-LD ở trang bài khác với BXH | Lấy được bài/thời lượng, không có counter trong mẫu; cấu hình số comment hiển thị không phải số comment thật | **Chưa tìm được nguồn nhanh đã xác minh**; BXH đang có cập nhật tuần |
| **JioSaavn** | API song-details, trending, thêm permalink bài công khai | Cache trước có `play_count` của 1 bài và 3 bài trending; các API và trang mới đều lỗi kết nối `10061` | **Có counter nhưng chưa xác minh nhịp**, chưa thu được snapshot mới trong lượt này |
| **Kugou** | BXH tăng trưởng `6666`; mobile rank-info và rank-list | HTML có 22 bài, không đổi qua các lần lấy; hai mobile endpoint trả cấu hình chuyển tới app, không trả danh sách bài | **Chưa xác minh nhịp nhanh**; ngày cập nhật không chứng minh realtime |
| **TIDAL** | Official OpenAPI album, thêm trang nghệ sĩ công khai | API trả 401 thiếu Authorization; trang nghệ sĩ HTTP 200 có metadata | **API cần Bearer; chưa xác minh nguồn activity realtime công khai** |
| **Qobuz** | RSS phát hành mới; thử trang Top 50 khác vùng | RSS có 40 album, không đổi trong khoảng 12 phút; URL US Top 50 thực tế trả danh mục playlist; Top 50 chính thức vùng CH mô tả theo tháng | **Có feed phát hành mới**, chưa xác minh nhịp nhanh; chưa có realtime nghe nhạc |
| **Jamendo** | Feeds, track-reviews mới nhất, recent-tracks với stats và `type` hợp lệ | Cả 3 trả HTTP 200 nhưng API `code=11`, test client bị suspended, `results=[]` | **Chưa lấy được nguồn nhanh**; docs mô tả stats cập nhật ngày, review/feed chưa nêu độ trễ |
| **Internet Archive** | Advanced Search xếp `publicdate desc`, lấy hai lần | 20 mục công khai gần nhất, có ngày thêm/download; danh sách không đổi trong khoảng 12 phút | **Theo dõi được mục nhạc mới**; chưa đo độ trễ lập chỉ mục, không phải thống kê lượt nghe |
| **Amazon Music** | Official `/v2/tracks/top` thay cho HTML playlist | API trả 401 `INVALID_TOKEN`; docs yêu cầu OAuth, `x-api-key`, quyền developer | **API cần quyền; chart cadence chưa xác minh**; lịch sử cá nhân là dữ liệu khác |
| **Anghami** | Charts, mobile Top Anghami, trang bài mới; tài liệu Live Radio | 3 trang thử mới trả 406. Live Radio có bình luận/applause realtime trên app; BXH Top Anghami cập nhật ngày | **Có tính năng realtime trên mobile nhưng chưa crawl được feed** |
| **JOOX** | Song URL Thái Lan, thêm BXH Hong Kong | Cả hai chuyển tới trang giới thiệu `/intl`, không có catalog/BXH dùng được | **Chưa xác minh**, cách crawl này chưa trả dữ liệu |
| **Kuwo** | Chart API trực tiếp và trang rankList | API HTTP 200 nhưng `success:false`, “The request is illegal!”; trang rankList timeout | **Chưa xác minh**, request chưa được nguồn chấp nhận |
| **ChiaSeNhac** | Domain gốc và `www` | Cả hai lỗi phân giải DNS trên kết nối kiểm tra | **Chưa xác minh**, chưa đọc được nguồn |
| **Napster** | App chính thức, legacy top-tracks API, kiểm tra tài liệu hiện tại | App trả shell; legacy API lỗi DNS. Tài liệu hiện tại tập trung AI companions | **Chưa có nguồn realtime âm nhạc dùng được**; API realtime AI không tính là dữ liệu nghe nhạc |

## Dữ liệu và đối chiếu

- Toàn bộ thử mạng: `out/realtime-audit-2026-10-07/round1/summary.json` và `round2/summary.json`, phần **`probe_attempts`** có đủ 21 nguồn và 61 lần thử, gồm cả lỗi không nhận được HTTP.
- Phản hồi gốc, URL sau chuyển hướng, timestamp: `round1/http/`, `round2/http/`.
- Mẫu đã parse: **202 bản ghi vòng 1, 232 vòng 2**, gồm metadata, bình luận, giao dịch và nguồn archive; đây không phải số bài khác nhau. Mẫu JSONL từ API initial Bandcamp được giới hạn 100 mục/lần; JSON gốc giữ đủ 385 mục incremental.
- So sánh số liệu, timestamps, delta và phân loại sản phẩm: `out/realtime-audit-2026-10-07/comparison.json`.
- Đã đọc JSONL và đối chiếu tổng từng nền tảng với summary. Nhận HTTP 200 với `success:false`, `code=11` hoặc trang giới thiệu không được tính là crawl thành công.

```powershell
# Lượt mới để tránh dùng lại cache của lần trước.
python -X utf8 scripts/crawl-platform-survey.py --output-dir out/realtime-audit-new/round1 --probe-manifest tmp/realtime-audit/requests.json

# Phân tích lại hai vòng đã lưu; không gọi mạng.
python -X utf8 scripts/analyze-realtime-audit.py
```

Hai adapter `crawler/survey_audit_western.py` và `survey_audit_asian.py` đọc public HTML/API. Script so sánh sử dụng dữ liệu nghiệp vụ như ID/hạng/counter/giao dịch; thay đổi hash HTML đơn thuần không được coi là sự kiện mới. Thời gian `crawled_at` khi parse lại cache không thay thế thời gian request gốc.

## Căn cứ về quyền truy cập và chu kỳ

- Bandcamp: [trang artists](https://bandcamp.com/artists), [API initial công khai](https://bandcamp.com/api/salesfeed/1/get_initial), và [client JavaScript được trang nhúng](https://s4.bcbits.com/client-bundle/1/ArtistsLabelsPages_1/salesfeed_js-6850855cdc8c60f8112e736e8178db6d.js) xác nhận polling 32 giây, đệm UI 120 giây.
- Spotify: [Currently Playing](https://developer.spotify.com/documentation/web-api/reference/get-the-users-currently-playing-track), [Recently Played](https://developer.spotify.com/documentation/web-api/reference/get-recently-played) dùng OAuth và quyền tài khoản. [Live stream count](https://support.spotify.com/us/artists/article/live-stream-count/) là tính năng Spotify for Artists, không phải dữ liệu public embed/Kworb.
- Apple Music: [Generating developer tokens](https://developer.apple.com/documentation/applemusicapi/generating-developer-tokens), [Recent played tracks](https://developer.apple.com/documentation/applemusicapi/get-v1-me-recent-played-tracks).
- SoundCloud: [API documentation](https://developers.soundcloud.com/docs) mô tả OAuth; [Insights troubleshooting](https://help.soundcloud.com/hc/en-us/articles/45764984867355-Your-Insights-troubleshooting) mô tả cache của số liệu công khai.
- TIDAL: [API authorization](https://developer.tidal.com/documentation/api-sdk/api-sdk-authorization). [My New Arrivals](https://support.tidal.com/hc/en-us/articles/29710779228817-My-New-Arrivals) là playlist cá nhân cập nhật thứ Sáu.
- Jamendo: [Tracks](https://developer.jamendo.com/v3.0/tracks) nói stats cập nhật ngày; [Reviews](https://developer.jamendo.com/v3.0/reviews/tracks), [Feeds](https://developer.jamendo.com/v3.0/feeds) cung cấp phương thức thử khác. Client thử được công bố trong [Authentication](https://developer.jamendo.com/v3.0/authentication).
- Amazon Music: [Top tracks API](https://developer.amazon.com/docs/music/API_web_track_v2.html), [Developer portal](https://developer.amazon.com/docs/music/landing_home), [Views/recent](https://developer.amazon.com/docs/music/API_web_views_overview_v2.html).
- Anghami: [Top Anghami](https://mobile.anghami.com/playlist/6471050) nêu daily; [Live Radio](https://support.anghami.com/hc/en-us/articles/6197975412890-Live-Radio) xác nhận realtime interaction và chỉ có trên mobile.
- Qobuz: [RSS đã crawl](https://www.qobuz.com/us-en/rss/new-releases/download-streaming-albums), [Top 50 chính thức vùng CH](https://www.qobuz.com/ch-fr/playlists/qobuz-top-50/2340483) mô tả cập nhật tháng.
- Napster: [Thông báo sản phẩm mới](https://www.napster.com/news/napster-unveils-new-app-experience-to-bring-ai-creations-and-video-companions-to-mobile-devices), [API reference hiện tại](https://developers.napster.com/docs/api-reference).

Không gán “không có realtime” cho toàn bộ nền tảng từ việc một endpoint trả lỗi, cần token hoặc mẫu catalog không có counter. Các hàng chưa xác minh thể hiện giới hạn của những phương thức và quyền truy cập đã thực sự kiểm tra.
