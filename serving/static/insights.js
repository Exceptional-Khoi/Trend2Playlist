'use strict';
const $ = s => document.querySelector(s);
const titles = {
 comments:["Khán giả nói gì?","Chọn bài hát để xem các chủ đề và cụm từ xuất hiện trong bình luận."],
 lifecycle:['Thứ hạng bài hát','Xem bài hát từng đứng thứ mấy và thứ hạng thay đổi ra sao.'],
 playlists:['Bài hát được nhiều playlist chọn','Khám phá những bài xuất hiện trong nhiều danh sách phát khác nhau.'],
 countries:['Bài hát nổi nhất từng quốc gia','Chọn một nước trên bản đồ để xem và nghe bài đứng đầu.']
};
const labels={insufficient_data:'Chưa đủ lịch sử',rising:'Tăng hạng',falling:'Giảm nhiệt',stable:'Ổn định',fluctuating:'Biến động',left_chart:'Rời BXH'};
let feature='lifecycle', requestId=0;
const fmt = x => x==null?'—':typeof x==='number'?x.toLocaleString('vi-VN',{maximumFractionDigits:4}):String(x);
const pct = x => x==null?'—':(x*100).toFixed(2)+'%';
function el(tag,text){const n=document.createElement(tag);if(text!=null)n.textContent=text;return n;}
function table(headers,rows){
 const box=el('section'), tools=el('div'), search=el('input'), prev=el('button','← Trước'), next=el('button','Sau →'), count=el('span');
 search.placeholder='Tìm trong bảng…';search.setAttribute('aria-label','Tìm trong bảng '+headers[0]);tools.className='table-tools';tools.append(search,prev,count,next);box.append(tools);
 const wrap=el('div');wrap.className='table-wrap';const t=el('table'),head=el('tr');headers.forEach(h=>head.append(el('th',h)));const th=el('thead');th.append(head);t.append(th);const body=el('tbody');t.append(body);wrap.append(t);box.append(wrap);
 let page=0;function draw(){const q=search.value.toLocaleLowerCase('vi');const matches=rows.filter(row=>row.some(v=>(v instanceof Node?v.textContent:fmt(v)).toLocaleLowerCase('vi').includes(q)));const pages=Math.max(1,Math.ceil(matches.length/50));page=Math.min(page,pages-1);body.replaceChildren();for(const row of matches.slice(page*50,page*50+50)){const tr=el('tr');for(const v of row){const td=el('td');if(v instanceof Node)td.append(v);else td.textContent=fmt(v);tr.append(td);}body.append(tr);}count.textContent=` ${matches.length} dòng · Trang ${page+1}/${pages} `;prev.disabled=page===0;next.disabled=page===pages-1;}
 search.addEventListener('input',()=>{page=0;draw();});prev.addEventListener('click',()=>{page--;draw();});next.addEventListener('click',()=>{page++;draw();});draw();return box;
}


const regions = new Intl.DisplayNames(['vi'], {type:'region'});
const region = c => /^[A-Z]{2}$/.test(c || '') ? regions.of(c) : '';
const platforms = {zing:'Zing MP3', spotify:'Spotify', apple_music:'Apple Music', youtube:'YouTube', kkbox:'KKBOX'};
function chartName(x) {
 const id=x.chart_id || '';
 const names={zing_new_release:'Bài mới phát hành',zing_realtime:'Đang thịnh hành', zing_week_vn:'Nhạc Việt trong tuần',spotify_top50_vn_daily:'Top 50 mỗi ngày',spotify_top_songs_vn_weekly:'Bài hát của tuần'};
 const name=names[id] || (/week/.test(id)?'Nghe nhiều trong tuần':'Nghe nhiều nhất');
 return [platforms[x.source] || x.source, name, region(x.country)].filter(Boolean).join(' · ');
}
function song(x) {
 const box=el('span',x.title || 'Chưa rõ tên');
 const artists=Array.isArray(x.artists)?x.artists.join(', '):x.artists;
 if(artists) box.append(el('small',artists));
 return box;
}
function listen(x) {
 if(!/^https?:\/\//i.test(x.url || '')) return 'Chưa có liên kết';
 const link=el('a','Nghe bài hát ↗');link.href=x.url;link.target='_blank';link.rel='noopener noreferrer';
 link.setAttribute('aria-label','Nghe '+x.title);return link;
}
async function showMap(items,id) {
 const data=await fetch('/static/world.json').then(r=>r.json());
 if(id!==requestId)return;
 const svgTag=t=>document.createElementNS('http://www.w3.org/2000/svg',t);
 const map=svgTag('svg');map.setAttribute('viewBox','0 0 360 150');map.setAttribute('aria-label','Bản đồ bài hát nổi nhất');
 const byCountry=new Map(items.map(x=>[x.country,x]));
 // World outlines are simplified from Natural Earth (datasets/geo-countries).
 const names=[];
 for(const [code,outline,center] of data){
  const path=svgTag('path');path.setAttribute('d',outline || ('M'+center.join(',')+'h0.8v0.8h-0.8Z'));const hit=byCountry.get(code);
  if(hit){path.classList.add('active');const tip=svgTag('title');tip.textContent=region(code)+' · '+hit.title;path.append(tip);
   path.onclick=()=>{$('#countries').value=code;load();};
   const label=svgTag('text');label.setAttribute('x',center[0]);label.setAttribute('y',center[1]);label.textContent=hit.title;

   const reveal=()=>{label.style.display='';};
   const restore=()=>{label.style.display=label.dataset.show==='true'?'':'none';};
   path.onpointerenter=reveal;path.onpointerleave=restore;
   path.setAttribute('tabindex','0');path.setAttribute('role','button');path.setAttribute('aria-label',tip.textContent);
   path.onfocus=reveal;path.onblur=restore;path.onkeydown=e=>{if(e.key==='Enter')path.onclick();};
   names.push(label);
  }
  map.append(path);
 }
 map.append(...names);$('#results').prepend(map);
 const occupied=[];
 for(const label of names){
  label.style.display='';
  const width=label.getBBox().width;
  label.setAttribute('x',Math.max(width/2+1,Math.min(359-width/2,Number(label.getAttribute('x')))));
  const box=label.getBBox();
  const fits=occupied.every(b=>box.x>b.x+b.width+2 || b.x>box.x+box.width+2 ||
                               box.y>b.y+b.height+1 || b.y>box.y+box.height+1);
  label.dataset.show=String(fits);
  if(fits)occupied.push(box);else label.style.display='none';
 }
}
async function load(){
 const id=++requestId;
 $('#heading').textContent=titles[feature][0];$('#description').textContent=titles[feature][1];
 $('#status').textContent='Đang tải…';const out=$('#results');out.replaceChildren();
 const params=new URLSearchParams({days:$('#days').value,countries:$('#countries').value});
 if($('#as-of').value)params.set('as_of',$('#as-of').value);
 try{
  if(feature==='comments'){
   params.delete('countries');params.set('sort',$('#comment-sort').value);
   const response=await fetch('/api/comments/tracks?'+params);
   if(!response.ok)throw Error();
   const data=await response.json();if(id!==requestId)return;
   if(!data.items.length){$('#status').textContent='Chưa có bình luận trong khoảng này. Hãy chọn khoảng thời gian dài hơn.';return;}
   params.set('as_of',data.as_of);
   const select=el('select'),label=el('label','Bài hát'),button=el('button','Phân tích bình luận'),report=el('section');
   for(const x of data.items)select.add(new Option(x.title,x.key));
   label.append(select);out.append(label,button,report);$('#status').textContent='Bình luận trong '+params.get('days')+' ngày đến '+new Date(data.as_of).toLocaleDateString('vi-VN',{timeZone:'UTC'})+'.';
   select.onchange=()=>report.replaceChildren();
   button.onclick=async()=>{
    button.disabled=true;select.disabled=true;report.replaceChildren(el('p','Đang đọc bình luận…'));
    try{
     const payload={track:select.value,sort:params.get('sort'),days:Number(params.get('days')),as_of:params.get('as_of')||null};
     const response=await fetch('/api/comments/report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
     const result=await response.json();if(!response.ok)throw Error(typeof result.detail==='string'?result.detail:'Chưa thể hoàn tất báo cáo.');
     const sorts={recent:'mới nhất',popular:'nổi bật',unknown:'chưa rõ cách chọn'};
     const date=ms=>new Date(ms).toLocaleDateString('vi-VN',{timeZone:'UTC'});
     report.replaceChildren(el('h3','Ý kiến khán giả'),el('p',result.summary),
      el('p',date(result.start_ms)+' – '+date(result.end_ms-1)+' · Nhóm bình luận '+result.sample_sorts.map(s=>sorts[s]||s).join(', ')));
     for(const [title,groups] of [['Chủ đề',result.topics],['Từ và cụm từ',result.keywords]]){
      if(!groups.length)continue;
      report.append(el('h3',title),table(['Nội dung','Bình luận','Tỷ lệ','Ví dụ'],groups.map(g=>{
       const details=el('details');details.append(el('summary','Xem trích đoạn'));
       g.evidence.forEach(e=>details.append(el('p',e.excerpt)));
       return [g.label+(g.description?' — '+g.description:''),g.count,Math.round(100*g.share)+'%',details];
      })));
     }
    }catch(e){report.replaceChildren(el('p',e.message));}
    finally{button.disabled=false;select.disabled=false;}
   };return;
  }
  const r=await fetch('/api/insights/'+feature+'?'+params);if(!r.ok)throw Error();
  const data=await r.json();if(id!==requestId)return;
  const rows=data.items;
  if(feature==='countries'){
   const select=$('#countries'), selected=select.value;
   select.replaceChildren(new Option('Tất cả quốc gia',''),...data.available_countries.map(c=>new Option(region(c),c)).sort((a,b)=>a.text.localeCompare(b.text,'vi')));
   if(selected && !data.available_countries.includes(selected))select.add(new Option(region(selected),selected));
   select.value=selected;
   out.append(table(['Quốc gia','Bài đứng đầu','Nghe nhạc'],rows.map(x=>[region(x.country),song(x),listen(x)])));
   await showMap(rows,id);
  }else if(feature==='playlists'){
   out.append(table(['Hạng','Bài hát','Số playlist','Nghe nhạc'],rows.map((x,i)=>[i+1,song(x),x.playlist_count,listen(x)])));
  }else{
   out.append(table(['Bài hát','Bảng xếp hạng','Hạng cao nhất','Xu hướng','Lịch sử','Nghe nhạc'],rows.map(x=>{
    const history=el('details');history.append(el('summary','Xem thứ hạng'),el('p',x.history.map(h=>(h.period || h.crawled_at?.slice(0,10) || '')+' · #'+h.rank).join(' → ')));
    return [song(x),chartName(x),x.best_rank,labels[x.status],history,listen(x)];
   })));
  }
  if(id===requestId)$('#status').textContent=rows.length?'':'Chưa có bài hát trong khoảng thời gian đã chọn.';
 }catch{if(id===requestId)$('#status').textContent='Chưa tải được kết quả. Vui lòng thử lại.';}
}
document.querySelectorAll('[data-feature]').forEach(b=>b.onclick=()=>{
 feature=b.dataset.feature;
 document.querySelectorAll('[data-feature]').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));
 $('.comment-filter').hidden=feature!=='comments';
 $('.country-filter').hidden=feature!=='countries';load();
});
$('#countries').onchange=load;$('#comment-sort').onchange=load;
$('#filters').onsubmit=e=>{e.preventDefault();load();};load();
