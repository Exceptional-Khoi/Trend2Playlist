# Kết quả crawl thử nền tảng nghe nhạc — 07/10/2026

Thực hiện trên branch `crawlKhoi`, thu thập dữ liệu công khai từ các dịch vụ cho phép nghe bài hát đầy đủ. Đây là lượt lấy mẫu có giới hạn, không phải danh mục toàn bộ dịch vụ âm nhạc trên thế giới và không phải toàn bộ catalog của từng nền tảng.

## Số liệu của lượt thử

- Đã thử **27 nguồn**: 26 dịch vụ/trang nghe nhạc và Internet Archive.
- **20 nền tảng nghe nhạc** trả dữ liệu dùng được; Internet Archive trả dữ liệu bổ sung.
- **1.935 bản ghi JSONL**: 1.874 từ các nền tảng nghe nhạc, 61 từ Internet Archive.
- File JSONL tổng cộng **2.050.447 byte**, khoảng 2,05 MB. Dữ liệu là metadata, bảng xếp hạng, playlist và bình luận; không tải file audio/video.
- Đã đọc lại toàn bộ JSONL và đối chiếu số dòng với thống kê từng nguồn. Các trường không có dữ liệu không được tính là đã thu thập.

**Đơn vị là bản ghi, không phải bài hát riêng biệt.** Một bài có thể xuất hiện trong nhiều BXH/playlist hoặc có nhiều bản ghi metadata. Các chỉ số của các nền tảng cũng không có cùng ý nghĩa: hạng, điểm, lượt nghe, lượt xem và lượt thích cần được xử lý riêng.

## Hai nguồn realtime vừa crawl thêm

| Nguồn đã thử | Kết quả mới | Dữ liệu lấy được | Nguồn cập nhật bao lâu? |
|---|---:|---|---|
| [QQ Music — BXH MV 201](https://y.qq.com/n/ryqq/toplist/201) | **20 MV** | VID, tên MV, nghệ sĩ, hạng, URL; thông báo chu kỳ cập nhật | **Mỗi 10 phút**, theo thông báo ngay trong dữ liệu nguồn |
| [KKBOX — BXH bài mới theo giờ, Taiwan](https://kma.kkbox.com/charts/hourly?terr=tw&lang=tc) | **50 bài** | ID, tên, nghệ sĩ, album, ảnh, ngày phát hành, hạng, URL, thời điểm nguồn cập nhật; 5 bài có lịch sử hạng 24 giờ | **Mỗi giờ**, theo phần giải thích cách tính BXH của nguồn |

Cả hai URL trả **HTTP 200** và đã xuất JSONL. QQ Music bị ngắt kết nối ở lượt đầu, lấy được khi thử lại. KKBOX trả 98 bài trong dữ liệu nhúng; lượt thử lấy mẫu 50 bài. Thời điểm KKBOX ghi đã cập nhật là **14:05:08 ngày 07/10/2026, giờ Việt Nam**; trang được lấy lúc 14:14:18.

Đây là **70 bản ghi mới**. Các mẫu cũ vẫn được giữ: QQ Music có tổng 40 bản ghi, KKBOX có tổng 100 bản ghi. Mẫu MV QQ không có lượt xem/thời lượng; mẫu KKBOX không có từng sự kiện nghe hoặc số lượt stream thô.

## Kết quả kiểm tra lại các nguồn từng ghi “Chưa rõ” hoặc “Không với mẫu này”

Lượt mới nhất đã kiểm tra **21 nguồn, 61 lần thử request**, gồm cả TIDAL, Jamendo, Napster và Kugou. Đã lấy hai snapshot để đối chiếu dữ liệu thật. [Bảng đầy đủ từng nguồn và bằng chứng](06-kiem-tra-lai-realtime-2026-10-07.md).

| Nguồn | Kết quả xác minh mới |
|---|---|
| Bandcamp | API bán nhạc công khai hoạt động; lấy được 385 mục bán mới từ cursor, gồm 304 mục nhạc. Client gọi mỗi 32 giây, đệm hiển thị 120 giây |
| Audiomack | Bộ đếm *Comfort* tăng 467 plays và 2 mục playlist trong khoảng 12 phút |
| NhacCuaTui | Bộ đếm *LAVIEM* tăng 16 lượt thích trong khoảng 12 phút |
| SoundCloud | Crawl được nội dung 26 bình luận và counter; plays thay đổi trong ngày, giữ nguyên giữa hai snapshot mới |
| NetEase | Hai BXH công khai gọi là realtime; đã thấy bài/hạng thay đổi trong ngày |

Độ trễ từ từng lượt nghe/lượt thích thật đến bộ đếm chưa được đo. Các API cá nhân cần token và tính năng realtime chỉ có trong app được ghi rõ trong bảng chi tiết. Không kết luận cả nền tảng thiếu realtime từ mẫu catalog hoặc một request thất bại.

Lượt kiểm tra mới lưu **202 bản ghi vòng 1, 232 vòng 2** tại `out/realtime-audit-2026-10-07/`, giữ riêng khỏi 1.935 bản ghi khảo sát chính. Lượt kiểm tra trước đó vẫn nằm tại `out/realtime-recheck-2026-10-07/`.

## Những nền tảng đã lấy được dữ liệu

**Cách đọc cột realtime:**

- **Có:** đã lấy được nguồn cập nhật nhanh; nhịp nguồn công bố được ghi trong ô.
- **Bộ đếm đã thay đổi:** đã đối chiếu được số liệu mới; chưa đo độ trễ từ sự kiện thật.
- **Cần quyền:** ghi cụ thể API/tính năng cần token hoặc tài khoản.
- **Chưa xác minh:** đã thử, nhưng chưa đủ bằng chứng về nguồn realtime dùng được.

Cột realtime dùng kết quả mới nhất. Số bản ghi và cột dữ liệu bên cạnh vẫn mô tả mẫu khảo sát chính 1.935 bản ghi; dữ liệu mới và điều kiện truy cập nằm trong báo cáo kiểm tra lại.

| Nền tảng | Bản ghi gốc | Thành phần mẫu gốc | Dữ liệu trong mẫu gốc | Realtime / kết quả kiểm tra mới nhất |
|---|---:|---|---|---|
| Zing MP3 | 648 | 152 mục BXH + 496 mục trong 5 playlist Top 100 | ID, tên bài, nghệ sĩ, album, thời lượng, ảnh, ngày phát hành, thể loại, hạng/hạng trước, điểm Zingchart; lượt nghe và thích ở 15 bản ghi đã enrich | **Có — #zingchart**; nhịp cập nhật chưa rõ |
| Spotify | 346 | 100 mục trong 2 BXH + 52 mục Hot Hits Vietnam; 194 mục stream qua Kworb | ID, tên bài/nghệ sĩ, URL, thời lượng ở 152 mục trực tiếp; hạng, hạng trước, stream ngày và tổng stream ở 194 mục nguồn phụ | **Cần OAuth — đang phát/lịch sử cá nhân**; BXH public ngày/tuần |
| Apple Music | 100 | Most Played Việt Nam | ID, tên, nghệ sĩ, hạng, thể loại ở 99/100 mục, ngày phát hành, ảnh, URL | **Cần token — API cá nhân/catalog**; nhịp RSS chưa xác minh |
| NhacCuaTui | 63 | 60 mục từ 12 BXH + 3 bài bổ sung | ID, tên, nghệ sĩ, thời lượng, thể loại, ảnh, thời điểm phát hành, thích/bình luận/chia sẻ; tên/ngày BXH, hạng và một phần hạng trước | **Bộ đếm đã thay đổi — +16 like/12 phút**; độ trễ chưa rõ |
| Nhac.vn | 20 | BXH bài hát Việt Nam | ID, tên bài/nghệ sĩ, hạng 1–20, ảnh, URL và tên BXH; không có thời lượng/lượt nghe trong mẫu | **Chưa xác minh nguồn nhanh** — BXH tuần, bài không có counter |
| YouTube — MV nhạc đầy đủ | 22 | 2 bản ghi metadata của 1 MV + 20 bình luận | Tên MV, kênh, thời lượng, lượt xem, ngày đăng, mô tả, ảnh; nội dung/thời gian/lượt thích bình luận và mã băm người bình luận | **Có — theo dõi bình luận mới**; chưa đo độ trễ |
| Deezer | 72 | 15 bài BXH + 25 bài tìm kiếm + 2 album + 15 nghệ sĩ + 15 playlist | ID, tên, nghệ sĩ/album, thời lượng, ảnh, URL, vị trí BXH, explicit; ISRC ở bài tìm kiếm | **Chưa xác minh** — đúng API comment/fan bị lỗi kết nối |
| SoundCloud | 3 | Metadata + oEmbed của cùng 1 bài; 1 playlist oEmbed | Tên, nghệ sĩ, thời lượng, thể loại, tags, ngày, ảnh, lượt nghe/thích/repost, số bình luận; chưa lấy nội dung bình luận | **Bộ đếm đổi trong ngày; crawl được 26 comment**; độ trễ chưa rõ |
| Bandcamp | 25 | 1 album + 24 bài | Tên bài/nghệ sĩ/album, thứ tự, thời lượng, URL; ngày phát hành, mô tả, giá và UPC của album | **Có — API giao dịch mua nhạc gần realtime** |
| Audiomack | 50 | 50 bài trên trang Trending | ID, tên, nghệ sĩ, thời lượng, thể loại, ngày, ảnh, lượt nghe/thích/repost, số playlist/bình luận, thông tin uploader; ISRC/UPC khi có | **Bộ đếm đã thay đổi — +467 plays/12 phút**; độ trễ chưa rõ |
| JioSaavn | 40 | Tìm bài của Arijit Singh | ID, tên, nghệ sĩ, album, thời lượng, ngôn ngữ, năm phát hành, ảnh, URL | **Chưa xác minh** — có API play_count, lần mới lỗi kết nối |
| NetEase Cloud Music | 100 | 100 bài từ BXH ID 19723756 | ID, tên, nghệ sĩ, album, thời lượng, hạng, điểm do nguồn cung cấp, ảnh, URL | **Có — BXH realtime khác đã kiểm tra**; BXH 19723756 vẫn chưa rõ |
| Melon | 100 | TOP100 Hàn Quốc | ID, tên, nghệ sĩ, album, hạng, ảnh, URL; loại bỏ lượt thích placeholder ở HTML | **Có — BXH mỗi giờ** |
| Bugs | 100 | BXH Hàn Quốc | ID, tên, nghệ sĩ, album, hạng, ảnh, URL | **Có — BXH có bản theo giờ** |
| QQ Music | 40 | 20 bài BXH 4 + 20 MV BXH 201 | Bài: ID/mid, tên, nghệ sĩ, album, thời lượng, ảnh khi có, hạng/ngày BXH, giá trị tăng trưởng. MV: VID, tên, nghệ sĩ, hạng, URL, nhịp cập nhật | **Có — BXH MV mỗi 10 phút**; BXH bài 4 theo ngày |
| Kugou | 22 | 22 bài hiện trong HTML TOP500 | Tên, nghệ sĩ, thời lượng, vị trí BXH, ngày BXH, URL; không giả định đã lấy đủ 500 bài | **Chưa xác minh** — BXH khác không đổi; mobile API không trả bài |
| KKBOX | 100 | 50 bài BXH ngày 06/10/2026 + 50 bài mới BXH theo giờ, Taiwan | ID, tên, nghệ sĩ, album, ngày phát hành, ảnh, hạng, lãnh thổ, URL; BXH ngày có hạng kỳ trước; BXH theo giờ có thời điểm cập nhật và lịch sử hạng 24 giờ ở 5 bài | **Có — BXH bài mới mỗi giờ**; giữ thêm mẫu BXH ngày |
| TIDAL | 16 | Album After Hours: 14 bài + album + nghệ sĩ | ID bài, tên, thứ tự, thời lượng, explicit, album/nghệ sĩ; album có ngày phát hành và ảnh | **Chưa xác minh nguồn nhanh** — OpenAPI cần Bearer |
| Qobuz | 6 | Album Kind Of Blue: 5 bài + album | ID, tên/nghệ sĩ/album, thời lượng, credits, hãng phát hành, thể loại, chất lượng âm thanh, giá; album có ngày phát hành | **RSS album mới** — nhịp nhanh chưa xác minh |
| Jamendo | 1 | Trang bài Upbeat | Tên bài, nghệ sĩ, mô tả, ảnh, URL và metadata trang; chưa có catalog/BXH/thời lượng qua API | **Chưa xác minh** — 3 API lỗi client suspended; stats theo ngày |

## Nguồn nhạc bổ sung

| Nguồn | Bản ghi | Dữ liệu | Realtime / nhịp cập nhật |
|---|---:|---|---|
| Internet Archive / Live Music Archive | 61 | 20 mục lưu trữ + metadata chi tiết 1 mục + 40 metadata file âm thanh; tên/nghệ sĩ/ngày biểu diễn, số download, mô tả, định dạng, kích thước và thời lượng khi có | **Mục nhạc mới** — độ trễ lập chỉ mục chưa đo |

Internet Archive được tính riêng vì là kho lưu trữ, không phải dịch vụ streaming thông thường. Các file âm thanh chỉ được đọc metadata từ API, không tải nội dung file.

## Nguồn chưa lấy được dữ liệu bài hát

| Nguồn | Kết quả thực tế của lượt thử | Realtime / nhịp cập nhật |
|---|---|---|
| Amazon Music | Homepage và trang album trả HTML nhưng không có metadata catalog/bài hát parse được | **API cần quyền developer/OAuth**; nhịp chart chưa xác minh |
| Anghami | Trang bài hát trả HTTP 406 | **Live Radio có realtime trên mobile**; chưa crawl được feed |
| JOOX | URL BXH Thái Lan chuyển về trang marketing `/intl`, không trả danh sách bài hát | **Chưa kiểm tra được** |
| Kuwo | URL BXH trả HTTP 500; dữ liệu nhúng rỗng | **Chưa kiểm tra được** |
| ChiaSeNhac | Không phân giải được DNS của `chiasenhac.vn` trên kết nối thử | **Chưa kiểm tra được** |
| Napster | Trang chủ không trả metadata bài hát/album dùng được | **Chưa có nguồn realtime nhạc dùng được**; sản phẩm/API hiện tại về AI |

Đây là kết quả của các URL và kết nối đã thử, không chứng minh rằng mọi cách truy cập các dịch vụ đó đều thất bại.

## Các giới hạn cần giữ khi sử dụng dữ liệu

- **Realtime:** gọi lại nguồn mỗi phút không làm BXH mỗi giờ thành dữ liệu mỗi phút. Muốn theo dõi thay đổi, crawler cần gọi lại trang/API định kỳ. Chưa thu được luồng từng lượt nghe hoặc đo độ trễ từ lượt nghe đến BXH.
- **QQ Music / KKBOX:** nhịp 10 phút/1 giờ là nhịp nguồn công bố. QQ MV không có timestamp cập nhật trong phản hồi này. KKBOX có timestamp và giải thích BXH tổng hợp lượng thành viên nghe trong giờ trước; không phải nhật ký từng lượt nghe. Lịch sử 24 giờ được lưu bên trong 5 bản ghi, không cộng thành bản ghi bài hát mới.
- **Phạm vi quyền truy cập:** các dashboard riêng của nghệ sĩ có thể có thống kê realtime nhưng cần tài khoản/quyền sở hữu; không áp dụng khả năng đó cho nguồn công khai đã crawl.
- **Spotify:** 152 bản ghi lấy trực tiếp từ embed Spotify; 194 bản ghi stream lấy qua **Kworb**, trang tổng hợp. Mỗi bản ghi giữ `source_url` để phân biệt nguồn.
- **YouTube:** Charts API trả HTTP 429. Đã lấy metadata và 20 bình luận từ MV chính thức *Lạc Trôi* qua trang watch/oEmbed; chưa lấy BXH YouTube trong lượt này. Không tính YouTube và YouTube Music thành hai nền tảng thành công riêng.
- **Jamendo:** mã client dùng thử trong tài liệu chính thức hiện trả lỗi ứng dụng bị đình chỉ. Chỉ tính metadata trang bài hát đã đọc thành công.
- **SoundCloud:** 3 bản ghi không tương ứng 3 bài; có hai cách mô tả cùng một bài và metadata một playlist.
- **Deezer:** `rank` là chỉ số do API cung cấp, không phải số lượt stream. Vị trí trên danh sách lấy mẫu được giữ ở `chart_position`.
- **Audiomack:** `source_position` là thứ tự trong trang Trending; không coi đó là hạng BXH được nền tảng công bố.
- **Bandcamp:** không dùng `play_count=0` trong cấu hình trang làm số lượt nghe thực tế.
- **Nhac.vn:** dữ liệu BXH công khai không tự chứng minh tính cập nhật của BXH; cần đánh giá thêm trước khi dùng làm tín hiệu xu hướng hiện tại.
- Dữ liệu trải trên nhiều thị trường. Các bảng Hàn Quốc, Trung Quốc, Taiwan và catalog quốc tế chưa được lọc riêng cho Việt Nam.

## File kết quả và chạy lại

- Thống kê máy đọc: `out/crawlKhoi-survey/summary.json`.
- Dữ liệu: `out/crawlKhoi-survey/records/<platform>.jsonl`.
- HTTP gốc và metadata phản hồi: `out/crawlKhoi-survey/http/`.
- Kết quả kiểm tra lại endpoint thay thế: `out/realtime-recheck-2026-10-07/summary.json` và `records/`.
- Lượt kiểm tra đầy đủ mới nhất: `out/realtime-audit-2026-10-07/round1/`, `round2/`, và `comparison.json`; chi tiết trong [report kiểm tra lại](06-kiem-tra-lai-realtime-2026-10-07.md).
- Lịch sử các lần sửa parser/chạy lại: `out/crawlKhoi-survey/attempt-history.jsonl`.
- Runner: `scripts/crawl-platform-survey.py`.
- Adapter: `crawler/survey_primary.py`, `survey_asian.py`, `survey_western.py`, `survey_other.py`, `survey_extra.py`; hai parser mới `survey_realtime_qq.py`, `survey_realtime_kkbox.py` được gọi trong nhóm `extra`; các endpoint kiểm tra lại dùng `survey_recheck_sources.py`.

```powershell
# Cài dependencies vào môi trường Python đang dùng nếu chưa có.
python -m pip install -r crawler/requirements.txt

# Lượt crawl mới dùng thư mục mới, tránh ghi trùng mẫu đã có.
python -X utf8 scripts/crawl-platform-survey.py --output-dir out/crawlKhoi-new-run

# Đọc lại HTTP đã thu được để sửa parser, không gửi request mạng.
python -X utf8 scripts/crawl-platform-survey.py --output-dir out/crawlKhoi-survey --groups asian --platforms 'NhacCuaTui,Nhac.vn' --resume --offline

# Lấy lại hai nguồn QQ Music/KKBOX, gồm cả mẫu ngày và mẫu cập nhật nhanh.
python -X utf8 scripts/crawl-platform-survey.py --output-dir out/crawlKhoi-survey --groups extra --platforms 'qq_music,kkbox' --resume --refresh
```

HTTP được cache theo method, URL và body request. `--resume --platforms` thay thế dữ liệu mẫu của các nguồn được chọn và giữ các nguồn khác. Thêm `--refresh` để lấy phản hồi mới. Các thư mục `out/` nằm trong `.gitignore`, nên dữ liệu thô không tự được đưa vào commit.

Trong quá trình đối chiếu đã sửa phép tính hạng trước của Spotify/Kworb: hạng hiện tại 1 và tăng 3 bậc phải có hạng trước 4. Đã xác nhận lại bằng hàng HTML thực tế và bản ghi xuất ra.

## Một số nguồn công khai đã dùng

- [Zing MP3](https://zingmp3.vn/), [NhacCuaTui](https://www.nhaccuatui.com/), [Nhac.vn BXH](https://nhac.vn/chart).
- [Spotify Top 50 Vietnam embed](https://open.spotify.com/embed/playlist/37i9dQZEVXbLdGSmz6xilI), [Kworb Spotify Vietnam](https://kworb.net/spotify/country/vn_daily.html), [Apple Music feed](https://rss.applemarketingtools.com/api/v2/vn/music/most-played/100/songs.json).
- [MV chính thức Lạc Trôi](https://www.youtube.com/watch?v=Llw9Q6akRo4), [Deezer chart API](https://api.deezer.com/chart/0?limit=15), [SoundCloud oEmbed docs](https://developers.soundcloud.com/docs/oembed).
- [Bandcamp album](https://c418.bandcamp.com/album/minecraft-volume-alpha), [Audiomack Trending](https://audiomack.com/trending-now/songs).
- [TIDAL album embed](https://embed.tidal.com/albums/134858516), [Qobuz album](https://www.qobuz.com/gb-en/album/kind-of-blue-miles-davis/qe7yczkjg0zxc), [Jamendo testing read API documentation](https://developer.jamendo.com/v3.0/authentication).
- URL cụ thể của tất cả các nguồn và trạng thái phản hồi được giữ trong `summary.json` và thư mục `http/`.

## Căn cứ bổ sung cho cột realtime

- **Zing:** phản hồi API đã lưu có `RTChart`, `chart.times` và điểm theo từng giờ. Đây là bằng chứng về dữ liệu BXH theo thời gian; chưa phải phép đo độ trễ.
- **YouTube:** [CommentThreads.list](https://developers.google.com/youtube/v3/docs/commentThreads/list) hỗ trợ đọc bình luận theo thời gian; [YouTube giải thích cách cập nhật chỉ số](https://support.google.com/youtube/answer/2991785) cho biết thời điểm thay đổi bộ đếm có thể khác nhau và số liệu có thể tạm dừng/được điều chỉnh.
- **Spotify:** [Understanding Spotify charts](https://support.spotify.com/us/artists/article/understanding-spotify-charts/) mô tả kỳ BXH ngày/tuần. Độ trễ bổ sung của Kworb chưa được đo.
- **Melon:** tooltip trên [TOP100](https://www.melon.com/chart/index.htm) và bản HTML đã thu xác nhận cập nhật mỗi giờ.
- **Bugs:** [BXH chính thức](https://music.bugs.co.kr/chart) có các snapshot realtime chọn theo giờ; chưa xác nhận lịch xuất bản liên tục suốt ngày.
- **Nhac.vn:** [Trang BXH](https://nhac.vn/chart) và HTML đã thu ghi lịch cập nhật hàng tuần vào thứ Hai.
- **SoundCloud:** [Insights troubleshooting](https://help.soundcloud.com/hc/en-us/articles/45764984867355-Your-Insights-troubleshooting) nêu khả năng số liệu trang track/profile bị cache.
- **Audiomack:** [Giải thích về play counts](https://audiomack.zendesk.com/hc/en-us/articles/360054835331-Why-can-I-no-longer-see-my-plays-or-why-have-my-total-plays-decreased) mô tả số lượt nghe có thể biến động, tạm dừng hoặc được điều chỉnh.
- **QQ Music:** [BXH MV 201](https://y.qq.com/n/ryqq/toplist/201) đã lấy được 20 MV; trường `data.updateTips` ghi `每10分钟更新` (mỗi 10 phút). HTTP gốc: `http/bc3567961965a8ec235d.body`. [BXH bài 4](https://y.qq.com/n/ryqq/toplist/4) vẫn là nguồn theo ngày.
- **KKBOX:** [BXH bài mới theo giờ](https://kma.kkbox.com/charts/hourly?terr=tw&lang=tc) đã parse được; trường `hourly_chart_note` ghi tự sắp xếp/cập nhật mỗi giờ. `chart.updated_at = 1791356708`, tương ứng 07:05:08 UTC ngày 07/10/2026. HTTP gốc: `http/7d46660e54118060373d.body`. [BXH daily](https://kma.kkbox.com/charts/daily/song?terr=tw) được giữ riêng.
- **NetEase:** [实时热度榜](https://music.163.com/discover/toplist?id=8246775932) và [实时分享榜](https://music.163.com/discover/toplist?id=18176153161) đã parse được 20 bài; HTML gọi trực tiếp đây là BXH realtime. Không suy ra chu kỳ nếu trang không công bố.
- **Qobuz:** trang phát hành mới công khai RSS tại `/rss/new-releases/download-streaming-albums`; đã parse 40 mục có `pubDate`. Đây là feed catalog mới, không phải lượt nghe trực tiếp.
- **Bandcamp:** API bán nhạc công khai đã hoạt động; các snapshot, cursor, thời điểm giao dịch và đệm hiển thị được đối chiếu trong [report kiểm tra lại](06-kiem-tra-lai-realtime-2026-10-07.md).
- **JioSaavn / Internet Archive:** cache JioSaavn trước có counter nhưng các request mới lỗi kết nối; Advanced Search vẫn trả mục archive mới nhất. Không có cam kết về độ trễ từ nguồn.
- **Nguồn chưa có lịch cập nhật rõ:** ghi **Chưa xác minh** hoặc ghi riêng bộ đếm đã thay đổi; không suy ra realtime từ ngày trong `chart_date` hoặc từ việc endpoint đọc được bằng HTTP.
