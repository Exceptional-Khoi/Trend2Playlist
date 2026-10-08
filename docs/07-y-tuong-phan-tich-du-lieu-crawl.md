# Báo cáo 7 ý tưởng phân tích dữ liệu crawl

Cập nhật ngày **08/10/2026** theo các quyết định đã thống nhất. Đây là thiết kế chức năng và đánh giá dữ liệu để triển khai; các hàm phân tích chưa được cài đặt trong lần cập nhật này. Riêng ý tưởng 4 đã được thử bằng các lượt crawl thật, gồm nguồn bổ sung, và lưu dữ liệu mẫu.

Các ý tưởng 1, 3, 4, 5 và 6 dùng thống kê và quy tắc. Ý tưởng 7 gọi API của một LLM có sẵn để đọc bình luận; **không huấn luyện hoặc fine-tune mô hình ML/DL**.

## 1. Quyết định với từng ý tưởng

| Ý tưởng | Trạng thái | Nội dung sau khi cập nhật |
|---|---|---|
| 1 | **Chốt** | Phân tích vòng đời bài hát từ lịch sử BXH |
| 2 | **Bỏ** | Không triển khai phân tích nền tảng dẫn đầu và độ trễ lan tỏa |
| 3 | **Chốt** | Phân tích mối liên hệ giữa sự xuất hiện/vị trí trong playlist và thứ hạng BXH |
| 4 | **Đổi sang country; đã thử crawl** | So sánh độ phổ biến bài hát và mức giao nhau của BXH giữa các quốc gia; khả thi cho bản đầu bằng Spotify qua Kworb |
| 5 | **Để bàn bạc sau** | Theo dõi độ lan rộng của bài qua các tỉnh; chưa đưa vào phạm vi triển khai đã chốt |
| 6 | **Chốt** | Phân tích phản hồi play/skip/like trong ứng dụng; đo thời gian nghe thực tế cần bổ sung |
| 7 | **Đổi hướng: gọi API LLM** | Rút ra từ khóa và chủ đề khán giả đang bàn luận từ bình luận đã crawl |

## 2. Dữ liệu nào dùng được?

| Nhóm | Thông tin hiện có | Cách hiểu |
|---|---|---|
| BXH (`music.charts`) | Bài, nghệ sĩ, ID nền tảng, hạng, hạng trước nếu nguồn cung cấp, lượt xem/stream ở một số nguồn, thời điểm crawl | Phản ánh BXH của một nền tảng và thị trường cụ thể |
| Playlist (`music.playlists`) | ID/tên playlist, bài, vị trí, thể loại gợi ý, thời điểm crawl | Cho biết bài nằm ở đâu trong các playlist được theo dõi; chưa có số người tiếp cận |
| Google Trends (`music.trends`) | Mức quan tâm tìm kiếm theo tỉnh, bài mốc, độ phủ | Tín hiệu tìm kiếm; dùng cho ý tưởng 5 nếu tiếp tục bàn bạc |
| Bình luận (`music.comments`) | Nội dung, thời điểm đăng, lượt thích, bài/video, `comment_id`, mã tác giả ẩn danh | Là mẫu bình luận từ các video được chọn, gồm chế độ lấy mới nhất hoặc phổ biến |
| Sự kiện ứng dụng (`music.events`) | Bài, tỉnh, play/skip/like, thời điểm, `listen_ms`, `source` | Đến từ ứng dụng web hoặc chương trình giả lập, không phải lịch sử nghe cá nhân crawl từ Spotify |
| Mẫu country mới | BXH Spotify qua Kworb, Apple Music và YouTube ở những nước lấy thành công | Đã lưu riêng để kiểm tra ý tưởng 4; chưa tích hợp vào pipeline chính |

**Snapshot** là bản chụp dữ liệu ở một lần hoặc khoảng thời gian crawl. Ví dụ crawl lúc 10:00 và 10:30 có thể tạo hai snapshot, dù BXH tuần của nguồn vẫn chưa thay đổi. Vì vậy phải phân biệt **thời điểm mình lấy dữ liệu** với **ngày/tuần mà BXH của nguồn đại diện**.

## 3. Chi tiết 7 ý tưởng

### Ý tưởng 1 — Vòng đời bài hát: CHỐT

**Hàm đề xuất:** `analyze_track_lifecycle(track_key, window_days=30)`

**Câu hỏi trả lời:** bài đang bắt đầu được chú ý, tiếp tục tăng, duy trì ổn định hay giảm nhiệt?

**Đầu vào:** lịch sử BXH của từng bài, từng nguồn; hạng; thời điểm crawl; ngày/kỳ BXH nếu có. Dùng dữ liệu raw đầy đủ, vì view lịch sử hiện tại chỉ giữ lịch sử của những bài đang nằm trong top 10 của các BXH chính.

**Cách làm dễ hiểu:**

1. Trong từng BXH, lấy lần đầu hệ thống quan sát được bài, hạng tốt nhất và diễn biến hạng qua các kỳ.
2. Gộp những lần crawl lặp lại cùng một kỳ nguồn; không đếm một BXH tuần được crawl 48 lần thành 48 tuần.
3. Gán nhãn theo luật: mới quan sát, tăng hạng trong ít nhất hai kỳ, ổn định khi dao động nhỏ, giảm nhiệt khi hạng xấu đi liên tiếp.
4. Khi bài biến mất, kiểm tra lần crawl tiếp theo có thành công và đủ dữ liệu trước khi ghi nhận bài đã rời BXH.

**Đầu ra:** lịch sử hạng, hạng tốt nhất, thời gian theo dõi, số kỳ được quan sát, trạng thái hiện tại và lý do gán trạng thái.

**Ví dụ minh họa:** “Bài A tăng từ hạng 40 lên 12 qua 3 kỳ BXH ngày; hiện đang tăng.” Ví dụ này giải thích chức năng, không phải kết quả crawl mới.

**Khả thi:** pipeline lưu được lịch sử raw; cần kiểm tra và tích lũy đủ các kỳ nguồn trước khi phân loại vòng đời. Cần lưu rõ ngày/kỳ của nguồn và phân biệt “lần đầu hệ thống thấy bài” với ngày phát hành hoặc lần đầu bài thật sự vào BXH.

### Ý tưởng 2 — Nền tảng dẫn đầu và độ trễ: BỎ

Ý tưởng cũ tìm nền tảng nào ghi nhận một bài sớm nhất và khoảng trễ trước khi bài xuất hiện ở nền tảng khác. Theo quyết định của bạn, **loại khỏi phạm vi triển khai**. Giữ mục này trong báo cáo để lưu quyết định; không đưa vào danh sách ưu tiên.

### Ý tưởng 3 — Playlist và thứ hạng BXH: CHỐT

**Hàm đề xuất:** `analyze_playlist_exposure(window_days=30, genre=None)`

**Câu hỏi trả lời:** bài xuất hiện trong nhiều playlist hoặc ở vị trí cao có thường đi cùng thứ hạng tốt hơn sau đó không?

**Đầu vào:** lịch sử bài trong playlist, vị trí, ID playlist, thời điểm crawl và lịch sử BXH ở những kỳ sau.

**Cách làm dễ hiểu:**

1. Đếm số playlist khác nhau chứa bài trong từng thời điểm.
2. Tính điểm vị trí: `placement_score = tổng[1 / log2(position + 1)]`. Bài ở đầu playlist được nhiều điểm hơn bài ở cuối.
3. So mức xuất hiện/vị trí với thay đổi hạng sau 1, 3 hoặc 7 ngày; chỉ so các quan sát có đủ dữ liệu trước và sau.
4. Nhóm theo thể loại, nguồn hoặc loại playlist để giảm việc trộn những danh sách có mục đích khác nhau. Tách playlist biên tập khỏi playlist sao chép BXH để tránh so BXH với chính nó.

**Đầu ra:** số playlist có bài, điểm vị trí, thay đổi hạng sau từng khoảng thời gian và số quan sát dùng để so sánh.

**Ví dụ minh họa:** “Trong 60 bài được theo dõi, nhóm nằm ở đầu nhiều playlist có mức cải thiện hạng trung vị cao hơn nhóm còn lại.” Con số minh họa này chưa được tính từ dữ liệu thực tế.

**Khả thi:** cần tích lũy lịch sử playlist và BXH đủ dài. Chỉ số này đo sự hiện diện trong các playlist mình theo dõi; chưa đo số người thực sự thấy/nghe bài. Kết quả cho thấy mối liên hệ, chưa chứng minh playlist làm bài tăng hạng.

### Ý tưởng 4 — So sánh giữa các QUỐC GIA: ĐÃ KIỂM TRA DỮ LIỆU

**Hàm đề xuất:** `compare_country_tastes(countries, provider="spotify_kworb", chart_date=None, top_n=50)`

**Câu hỏi trả lời:** bài nào có thứ hạng nổi bật ở một quốc gia, và BXH của hai quốc gia có bao nhiêu bài chung?

Country ở đây là **thị trường của BXH**, ví dụ Việt Nam, Thái Lan, Indonesia, Mỹ, Nhật; không phải quốc tịch của nghệ sĩ hoặc nơi cư trú xác định của từng người nghe.

#### Dùng nguồn nào?

- **Spotify qua Kworb:** có ID Spotify, hạng, hạng trước ở một số bài, stream ngày, tổng stream và ngày BXH. Đây là dữ liệu từ nguồn tổng hợp Kworb, không phải một lượt gọi Spotify API trực tiếp.
- **Apple Music:** feed công khai theo thị trường có ID, tên bài, nghệ sĩ, thứ tự và thời điểm cập nhật. Không có số stream để tính thị phần nghe. Có thể tạo URL theo storefront bằng [RSS Builder của Apple](https://rss.marketingtools.apple.com/).
- **YouTube Charts:** có BXH theo quốc gia. [Tài liệu YouTube](https://support.google.com/youtube/answer/9014376?hl=en) mô tả Weekly Top Songs và các thị trường được hỗ trợ. Endpoint web đã thử trong lượt này bị giới hạn ở nhiều nước, nên chưa chọn làm nguồn bắt buộc.

**Bản đầu nên dùng Spotify qua Kworb cho cả 5 nước.** Apple dùng để đối chiếu thêm ở những nước lấy thành công. Chưa cần suy đoán quốc gia từ ngôn ngữ bình luận hay dùng Google Trends theo tỉnh cho chức năng này.

#### Cách tính insight

1. Chọn **cùng nền tảng, cùng loại BXH và cùng ngày/kỳ nguồn** giữa các quốc gia. Không so BXH Spotify ngày với BXH YouTube tuần như thể cùng một thước đo.
2. So cùng top N, ví dụ top 50, bằng ID bài của nền tảng. Đếm bài chung và tính `Jaccard = số bài chung / số bài trong hợp hai danh sách`.
3. Với cùng một bài, so hạng giữa các nước. Có thể chuẩn hóa `rank_score = (chart_size - rank + 1) / chart_size`, trong đó `chart_size` là độ dài BXH đầy đủ đã lấy của nước đó, không phải `top_n`. Trong mẫu Spotify này `chart_size = 200`; phép giao nhau vẫn chỉ xét top 50.
4. Với Spotify, tính `chart_stream_share = stream của bài / tổng stream các bài trong BXH đã lấy`. Chỉ số này là tỷ trọng trong **BXH được thu thập**, chưa phải tỷ trọng của toàn bộ lượt nghe trong quốc gia.
5. Trả lại những bài có vị trí nổi bật ở một nước so với các nước có quan sát cùng bài. Nếu bài không có trong top 200 của nước khác, ghi “không có trong top 200 đã crawl”; không gán lượt stream bằng 0.

**Đầu ra:** bảng hạng bài theo country, mức giao nhau giữa các BXH, tỷ trọng stream trong BXH và số liệu về nguồn/ngày/độ đủ của dữ liệu.

#### Kết quả thử crawl thật ngày 07/10/2026

Đã thử 15 cặp nguồn–quốc gia. Lượt chính chạy **22:45–22:46 giờ Việt Nam**; sau đó thử lại Apple ở Thái Lan và Nhật. Dữ liệu đã được kiểm tra ID, tên, nghệ sĩ, hạng liên tục/không trùng và chỉ số stream/view ở những nguồn có cung cấp.

| Quốc gia | Spotify qua Kworb | Apple Music | YouTube Top Songs |
|---|---:|---:|---:|
| Việt Nam (`VN`) | 200 bài, HTTP 200 | 100 bài, HTTP 200 | HTTP 429, chưa lấy được |
| Thái Lan (`TH`) | 200 bài, HTTP 200 | Lượt đầu timeout; thử lại HTTP 502 | HTTP 429, chưa lấy được |
| Indonesia (`ID`) | 200 bài, HTTP 200 | 100 bài, HTTP 200 | 100 bài, HTTP 200 |
| Mỹ (`US`) | 200 bài, HTTP 200 | 100 bài, HTTP 200 | HTTP 429, chưa lấy được |
| Nhật (`JP`) | 200 bài, HTTP 200 | Timeout cả hai lượt | HTTP 429, chưa lấy được |

**Tổng: 1.400 bản ghi**, gồm 1.000 Spotify, 300 Apple và 100 YouTube. Đây là số dòng BXH theo nguồn/quốc gia, không phải 1.400 bài khác nhau. **Cả 5 BXH Spotify cùng ghi ngày nguồn 05/10/2026**, dù được crawl ngày 07/10. Các dòng Spotify đều có ID, tên, nghệ sĩ, hạng, stream ngày và tổng stream; hạng trước có thể thiếu với bài mới xuất hiện.

Apple có thời điểm cập nhật feed, chưa có kỳ BXH ngày được xác định như Kworb. Mẫu YouTube Indonesia có hạng và weekly views, nhưng probe chưa trích được tuần nguồn cụ thể; vì vậy không dùng mẫu đó để kết luận so sánh cùng kỳ giữa quốc gia.

**Ví dụ dữ liệu thật:** cùng ID Spotify `20jbSiX29FDX4oQxBXyUEi`, bài “hate that i made you love me” — Ariana Grande, ngày BXH 05/10/2026:

| Quốc gia | Hạng | Stream trong ngày |
|---|---:|---:|
| Việt Nam | 190 | 22.945 |
| Thái Lan | 60 | 48.967 |
| Indonesia | 163 | 197.708 |
| Mỹ | 39 | 489.206 |

Từ bảng này có thể nói bài đứng hạng cao hơn ở Mỹ trong các thị trường được quan sát. Stream tuyệt đối chịu ảnh hưởng bởi quy mô thị trường, nên chưa đủ để kết luận người Mỹ thích bài nhiều hơn theo tỷ lệ dân số. Có thể kiểm tra các trang nguồn [Việt Nam](https://kworb.net/spotify/country/vn_daily.html), [Thái Lan](https://kworb.net/spotify/country/th_daily.html), [Indonesia](https://kworb.net/spotify/country/id_daily.html) và [Mỹ](https://kworb.net/spotify/country/us_daily.html); các trang này sẽ thay đổi theo ngày, còn dữ liệu mẫu bên dưới lưu lại lượt đã crawl.

Một phép so sánh đã tính được: **top 50 Spotify Việt Nam và Thái Lan có 1 ID bài chung**, Jaccard = `1 / 99 ≈ 0,0101`, trong cùng ngày nguồn. Kết quả mô tả hai BXH top 50; chưa đại diện cho toàn bộ gu âm nhạc của hai quốc gia. Các phiên bản/ID khác nhau của cùng một bài có thể làm số bài chung thấp hơn.

#### Bằng chứng và cách chạy lại

- [Tóm tắt crawl, số trường có dữ liệu và các so sánh](../out/country-feasibility-20261007T154533Z/summary.json).
- [Mẫu Spotify Việt Nam — 200 dòng](../out/country-feasibility-20261007T154533Z/records/spotify_kworb_vn.jsonl); [Thái Lan — 200 dòng](../out/country-feasibility-20261007T154533Z/records/spotify_kworb_th.jsonl).
- [Response gốc từ Kworb Việt Nam](../out/country-feasibility-20261007T154533Z/http/spotify_kworb_vn.html), kèm response của các nước khác trong cùng thư mục `http/`.
- [Kết quả thử lại Apple Thái Lan và Nhật](../out/country-feasibility-20261007T155422Z/summary.json).
- [Script thử crawl](../scripts/crawl-country-feasibility.py): lưu response gốc, JSONL chuẩn hóa và summary; không cần API key.

```powershell
python scripts\crawl-country-feasibility.py --countries vn,th,id,us,jp
```

Mỗi lần chạy tạo thư mục mới trong `out/`. Có thể chuẩn hóa lại response đã lưu mà không gọi mạng:

```powershell
python scripts\crawl-country-feasibility.py --reparse --output-dir out\country-feasibility-20261007T154533Z
```

#### Crawl thêm các nguồn khác — cập nhật 08/10/2026

Đã lưu response thật, chuẩn hóa các BXH lấy được và giữ lỗi của những nguồn chưa lấy được. Country là thị trường dữ liệu; với KKBOX, báo cáo dùng cả mã thị trường Đài Loan (TW) và Hồng Kông (HK).

| Nguồn | Dữ liệu mới lấy được | Dùng cho ý tưởng 4 như thế nào? |
|---|---|---|
| Apple Music | 200 dòng: Mỹ 100, Nhật 100 | So thứ tự bài giữa storefront. Gộp các lượt trước đã có mẫu VN, ID, US, JP; TH chưa lấy thành công |
| KKBOX | 200 dòng: TW, HK, SG, JP, mỗi thị trường 50; ngày BXH 06/10/2026 | ID, hạng hiện tại/hạng trước, bài/nghệ sĩ. Chỉ so cùng danh mục nhạc |
| RIAS — Singapore | 20 dòng BXH chính và 20 Regional, tuần 40: 25/09–01/10/2026 | Hạng, bài, nghệ sĩ, label, hướng thay đổi khi ảnh chỉ dẫn rõ. Giữ hai bảng riêng; mới có một nước |
| Google Trends — YouTube Search | 25 quan sát có dữ liệu: 3 cụm tìm kiếm tại 9 quốc gia | Tín hiệu quan tâm tìm kiếm bổ sung |
| Shazam | 5 URL country đều HTTP 405 | Chưa lấy được dòng dữ liệu bằng crawler hiện tại |
| Deezer | API và trang playlist US/GB/BR bị từ chối kết nối | Chưa có dữ liệu mới; chưa xác nhận crawl tự động khả thi |
| Official SEA Charts — hub | HTTP 200, nhưng HTML chưa có dòng bài hát trích được | Chưa lấy được BXH 6 nước qua hub. RIAS Singapore phía trên đã lấy được qua trang thành viên |
| JOOX Indonesia | Kết nối timeout | Chưa lấy được dữ liệu để chuẩn hóa |

**Tổng BXH mới: 440 dòng**, gồm 200 Apple + 200 KKBOX + 40 RIAS. Đây là số dòng theo nguồn/thị trường/lượt crawl, không phải 440 bài khác nhau. Trends được đếm riêng: 25 quan sát có tín hiệu. Response Trends trả 750 ô (250 mã địa lý × 3 từ khóa); các ô thiếu dữ liệu không được tính là có tín hiệu.

**Giới hạn KKBOX:** TW và SG là nhạc Hoa/Mandarin (category 297), HK là Local (320), JP là Japanese/domestic (733). Không so bốn bảng mặc định như bốn BXH tổng hợp cùng loại. Phép so sánh đã làm được: **TW–SG cùng danh mục Mandarin, ngày 06/10/2026, top 50 có 20 ID chung**, Jaccard = 20 / 80 = **0,25**. Nối bằng ID KKBOX giúp nhận ra cùng bài dù tên hiển thị dùng chữ giản thể/truyền thống khác nhau. Mẫu không có số stream.

**Hiểu Trends:** ba cụm đã lấy là “Seven Jung Kook”, “hate that i made you love me Ariana Grande”, “Earrings Malcolm Todd”. Response có chế độ PERCENTAGES. Ở Nhật, tỷ lệ lần lượt 45%, 34%, 21%; Indonesia 1%, 74%, 25%. Đó là tỷ lệ giữa **ba cụm được chọn trong từng nước**; không phải tỷ lệ người dân nghe bài và không cho biết nước nào có tổng lượng tìm kiếm lớn hơn. VN và TH không đủ dữ liệu cho ba cụm này. Ô has_data=false được lưu null; ô làm tròn 0 nhưng hiển thị “<1%” vẫn giữ là có tín hiệu.

**Khuyến nghị:** dùng Spotify qua Kworb cho bản so sánh 5 nước; Apple để đối chiếu storefront; KKBOX để so country trong cùng danh mục; RIAS bổ sung BXH tuần Singapore; Trends bổ sung tìm kiếm. Shazam, Deezer, hub SEA và JOOX chưa được xác nhận lấy dữ liệu tự động trong môi trường này. Một lượt thành công chưa chứng minh độ ổn định nhiều ngày.

Dữ liệu và script:

- [Tổng hợp dữ liệu chuẩn hóa, kiểm tra và so sánh](../out/country-additional-20261007T175501Z/summary.json).
- [Log HTTP và lỗi](../out/country-additional-20261007T175501Z/fetch-summary.json).
- [KKBOX TW](../out/country-additional-20261007T175501Z/records/kkbox_tw.jsonl), [SG](../out/country-additional-20261007T175501Z/records/kkbox_sg.jsonl), [HK](../out/country-additional-20261007T175501Z/records/kkbox_hk.jsonl), [JP](../out/country-additional-20261007T175501Z/records/kkbox_jp.jsonl).
- [RIAS Singapore — BXH chính](../out/country-additional-20261007T175501Z/records/rias_sg_national.jsonl), [Regional](../out/country-additional-20261007T175501Z/records/rias_sg_regional.jsonl).
- [Trends country — ô có dữ liệu](../out/country-additional-20261007T175501Z/records/google_trends_country_with_data.jsonl).
- [Lượt Apple mới](../out/country-feasibility-20261007T172112Z/summary.json), [Mỹ](../out/country-feasibility-20261007T172112Z/records/apple_music_us.jsonl), [Nhật](../out/country-feasibility-20261007T172112Z/records/apple_music_jp.jsonl).
- [Crawler bổ sung](../scripts/crawl-country-additional.py): không cần API key, giữ raw response. Chạy mới bằng python scripts\crawl-country-additional.py; lấy endpoint RIAS bằng --followup --output-dir <thư mục vừa tạo>; chuẩn hóa offline bằng --reparse --output-dir <thư mục>.

Dữ liệu mới được lưu riêng trong out/; chưa tích hợp vào Kafka/Spark/API của ứng dụng.

#### Kết luận tính khả thi và việc cần bổ sung

**Khả thi cho bản đầu so sánh BXH theo country bằng Spotify qua Kworb:** cả 5 quốc gia đều có 200 dòng đầy đủ trường chính và cùng ngày nguồn trong lượt thử. Lượt này xác nhận có thể lấy dữ liệu và tạo insight cơ bản; chưa kiểm chứng độ ổn định khi crawl nhiều ngày hoặc toàn bộ quốc gia.

Để tích hợp vào dự án cần:

1. Lưu country ISO, ngày/kỳ BXH và thời điểm crawl riêng. Truyền `chart_scope` đúng quốc gia; các crawler hiện có chỗ vẫn mặc định `VN`, dù URL đã đổi country. ID BXH YouTube cũng đang cố định hậu tố Việt Nam.
2. So Spotify giữa các nước bằng **Spotify track ID**. Bộ tạo `track_key` hiện bỏ ký tự ngoài ASCII, nên tên tiếng Thái/Nhật/Hàn có thể bị mất hoặc gộp nhầm. Nếu so khác nền tảng, cần xử lý Unicode và kiểm tra bản live/remix, các ID khác nhau của cùng bài.
3. Tạo view/API country riêng và điều chỉnh các bộ lọc nguồn; batch/API hiện có nhiều giả định Việt Nam. Không đưa BXH nước ngoài vào điểm toàn quốc Việt Nam.
4. Thu thêm lịch sử, ghi lỗi/độ mới của từng nguồn và chỉ so những nước có dữ liệu đủ, cùng kỳ. Không thay dữ liệu Spotify của một nước bị thiếu bằng Apple rồi trộn vào cùng phép so sánh.

### Ý tưởng 5 — Bài lan rộng qua các tỉnh: ĐỂ BÀN BẠC SAU

**Hàm dự kiến:** `analyze_track_geographic_spread(track_key, window_days=14)`

Ý tưởng là đếm số tỉnh có tín hiệu quan tâm rõ ràng với một bài qua các lần crawl Trends, rồi xem phạm vi tăng hay giảm. Ví dụ minh họa: bài từ 5 tỉnh có tín hiệu tăng lên 14 tỉnh.

**Trạng thái:** giữ lại để thảo luận; chưa triển khai và chưa coi là ưu tiên. Cần bàn thêm ngưỡng quan tâm, độ phủ dữ liệu và tác động của cửa sổ Trends 7 ngày trượt. Ý tưởng 4 đã chuyển sang country, còn ý tưởng 5 vẫn giữ phạm vi tỉnh như bản đề xuất cũ cho đến khi có quyết định khác.

### Ý tưởng 6 — Phản hồi nghe trong ứng dụng: CHỐT

**Hàm đề xuất:** `summarize_listening_feedback(window_days=7, source="web")`

**Câu hỏi trả lời:** bài nào được tương tác nhiều, bài nào thường bị bấm bỏ qua và nhóm bài nào có phản hồi tích cực hơn?

**Đầu vào:** sự kiện play/skip/like, bài, thời điểm, tỉnh và nguồn. `source="web"` là thao tác từ ứng dụng; `source="simulator"` là sự kiện do chương trình giả lập tạo.

**Làm được với dữ liệu hiện tại:** đếm từng hành động theo bài/thể loại/tỉnh/khung giờ, xem số người dùng ứng dụng đã tương tác, tỷ lệ thao tác skip trong play + skip và sự thay đổi theo thời gian. Ghi rõ đây là thống kê tương tác của mẫu người dùng ứng dụng; ID user chưa tự chứng minh có người thật riêng biệt.

**Ví dụ minh họa:** “Bài C có 100 lần bấm play và 30 lần bấm skip trong tuần; tỷ lệ thao tác skip là 30 / 130 ≈ 23,1%.” Đây là ví dụ về thao tác nút, chưa phải tỷ lệ bỏ qua của 130 phiên nghe thực tế.

**Điểm cần sửa so với đề xuất ban đầu:** `listen_ms` hiện **chưa đo thời gian nghe thật trên web**. API gán play bằng thời lượng cả bài, skip bằng 8 giây và like bằng 0; giao diện gửi sự kiện khi bấm nút. Simulator cũng tự tạo thời lượng theo luật giả lập. Vì vậy chưa tính được tỷ lệ nghe hết bài hoặc kết luận bỏ qua sớm từ những giá trị này.

**Nếu cần insight về nghe thực tế:** bổ sung đo thời gian ở trình phát và `playback_id` cho mỗi lần nghe. Khi đó mới tính `listen_ms / duration_ms`, tỷ lệ hoàn tất ≥ 80%, bỏ qua sớm và like gắn với lần nghe. Một phiên play rồi skip phải được tính là một phiên, không cộng thành hai lượt nghe độc lập.

**Khả thi:** chốt phần thống kê tương tác với dữ liệu hiện có. Phần thời lượng nghe/hoàn tất cần bổ sung ghi nhận thực tế; dữ liệu simulator được báo cáo riêng.

### Ý tưởng 7 — Từ khóa và chủ đề bình luận qua API LLM: ĐỔI HƯỚNG

**Hàm đề xuất:** `summarize_comment_topics_with_llm(track_key, window_days=7, max_comments=500)`

**Câu hỏi trả lời:** khán giả hay nhắc từ/cụm từ nào, và họ đang bàn luận về điều gì ở bài hoặc video đó?

Đây là ý tưởng thiết kế luồng xử lý. Đầu vào khi triển khai là **bình luận thực tế đã crawl**; các chủ đề ví dụ bên dưới chỉ minh họa, chưa phải kết quả đã chạy LLM. Sử dụng mô hình có sẵn qua API, không huấn luyện mô hình mới.

**Luồng đề xuất:**

1. Lấy bình luận theo bài và khoảng thời gian đăng; khử trùng bằng `comment_id`, bỏ nội dung rỗng và gắn số lượng mẫu. Ghi rõ mẫu lấy theo `recent` hay `popular` vì hai nhóm có thể khác nhau.
2. Chia bình luận thành các nhóm vừa giới hạn đầu vào của API. Giữ ID nội bộ làm bằng chứng; không cần gửi mã tác giả cho LLM.
3. Gọi LLM để trích các từ/cụm từ được nhắc và gom bình luận thành chủ đề dễ hiểu. Ví dụ: giọng hát, lời bài, hình ảnh MV, màn biểu diễn, câu chuyện nghệ sĩ hoặc việc nghe lại.
4. Yêu cầu kết quả JSON gồm từ khóa, chủ đề, mô tả ngắn và danh sách ID bình luận làm bằng chứng. Chỉ kết luận điều có trong đầu vào; đánh dấu không đủ bằng chứng khi cần.
5. Kiểm tra ID bằng chứng thuộc mẫu đã gửi, gộp từ khóa tương đương và tính số bình luận có nhắc bằng code. Với chủ đề do LLM phân loại, đếm ID bình luận duy nhất được gán vào từng chủ đề; không để LLM tự đoán số lần xuất hiện. Một bình luận có thể có nhiều chủ đề, nên tỷ lệ các chủ đề không nhất thiết cộng bằng 100%.
6. Gộp kết quả các nhóm thành báo cáo chung, giữ những từ khóa/chủ đề có nhiều bằng chứng và lưu thời điểm phân tích, cấu hình mô hình, phiên bản prompt và kích thước mẫu.

**Đầu ra dự kiến:**

| Thành phần | Nội dung |
|---|---|
| `keywords` | Cụm từ nguyên gốc, nhóm biến thể, số bình luận có nhắc, tỷ lệ trong mẫu |
| `topics` | Tên chủ đề, nội dung đang được bàn luận, số bình luận hỗ trợ |
| `evidence` | ID và trích đoạn ngắn từ bình luận trong mẫu |
| `summary` | Vài câu giải thích các chủ đề nổi bật, kèm số lượng mẫu và thời gian |

**Ví dụ minh họa:** “Trong 500 bình luận được phân tích, các từ/cụm từ hay nhắc là ‘điệp khúc’, ‘giọng hát’, ‘MV’. Khán giả chủ yếu bàn về đoạn điệp khúc dễ nhớ và hình ảnh trong MV.” Danh sách và con số này chưa được kiểm chứng; khi triển khai phải thay bằng kết quả có bằng chứng từ bình luận.

**Khả thi:** crawler đã có văn bản bình luận để làm đầu vào. Cần lựa chọn nhà cung cấp/mô hình, cấu hình API key, giới hạn chi phí/số bình luận và xử lý lỗi API trước khi chạy. Lần cập nhật report này chưa gọi LLM API. LLM hỗ trợ hiểu cách diễn đạt và gom chủ đề; keyword có thể kiểm tra trực tiếp, còn nhãn chủ đề vẫn cần xem lại mẫu để đánh giá chất lượng. Kết luận áp dụng cho **mẫu bình luận đã crawl**, chưa đại diện toàn bộ khán giả.

## 4. Phạm vi đề xuất tiếp theo

Triển khai ý tưởng **1, 3 và 6** theo quyết định đã chốt. Ý tưởng **4** có bằng chứng dữ liệu cho bản country đầu tiên bằng Spotify qua Kworb, nhưng cần cập nhật schema/nhận diện bài và view country. Ý tưởng **7** triển khai theo hướng API LLM khi đã có cấu hình provider; ý tưởng **2** đã bỏ và **5** giữ để bàn bạc sau.

Mỗi insight nên kèm nguồn, ngày/kỳ dữ liệu, số quan sát và lý do tính toán. Các phép thống kê giúp mô tả dữ liệu đã thu thập; khi nói về tác động của playlist, gu quốc gia hoặc chủ đề khán giả, cần giữ phạm vi kết luận tương ứng với mẫu thực tế.
