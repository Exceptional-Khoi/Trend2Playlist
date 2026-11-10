'use strict';
const $=s=>document.querySelector(s), el=(tag,text)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;return n;};
const titles={home:['🔥 Xu hướng nổi bật',''],lifecycle:['Thứ hạng bài hát',''],playlists:['Bài hát trong playlist',''],countries:['Bản đồ âm nhạc',''],comments:['Khán giả nói gì?','']};
const platforms={spotify:'Spotify',zing:'Zing MP3',apple_music:'Apple Music',itunes:'iTunes Store',youtube:'YouTube',kkbox:'KKBOX'};
const labels={insufficient_data:'Chưa đủ lịch sử',rising:'Tăng hạng',falling:'Giảm hạng',stable:'Ổn định',fluctuating:'Biến động',left_chart:'Rời BXH'};
const kinds={editorial:'Biên tập',chart_mirror:'Bản sao BXH',unknown:'Chưa phân loại',genre_collection:'Theo thể loại'};
const displayNames=new Intl.DisplayNames(['vi'],{type:'region'}),countryName=c=>{try{return displayNames.of(c)||c;}catch{return c;}};
const fmt=n=>n==null?'—':typeof n==='number'?n.toLocaleString('vi-VN',{maximumFractionDigits:2}):String(n);
const flag=c=>/^[A-Z]{2}$/.test(c||'')?String.fromCodePoint(...[...c].map(x=>127397+x.charCodeAt(0))):'◎';
const safeURL=u=>{try{const x=new URL(u);return ['https:','http:'].includes(x.protocol)?x.href:null;}catch{return null;}};
let feature='home',requestId=0,worldPromise,selectedCountry=null;const cache=new Map();
function params(){const p=new URLSearchParams({days:$('#days').value});if($('#as-of').value)p.set('as_of',$('#as-of').value);return p;}
async function api(path){const hit=cache.get(path);if(hit&&Date.now()-hit.time<30000)return hit.value;const promise=fetch(path).then(async r=>{const data=await r.json();if(!r.ok)throw Error(typeof data.detail==='string'?data.detail:'Chưa tải được dữ liệu.');return data;});cache.set(path,{time:Date.now(),value:promise});if(cache.size>24)cache.delete(cache.keys().next().value);try{return await promise;}catch(e){cache.delete(path);throw e;}}
function link(url,text){const href=safeURL(url);if(!href)return el('small','Chưa có liên kết từ nguồn');const a=el('a',text);a.href=href;a.target='_blank';a.rel='noopener noreferrer';return a;}
function song(x){const box=el('div');box.className='track-title';let art;const image=safeURL(x.thumbnail);if(image){art=el('img');art.src=image;art.loading='lazy';art.alt='';art.addEventListener('error',()=>{art.replaceWith(Object.assign(el('span','♫'),{className:'cover'}));},{once:true});}else art=el('span','♫');art.className='cover';const text=el('div');text.className='track-text';text.append(el('b',x.title||'Chưa rõ tên'),el('small',Array.isArray(x.artists)?x.artists.join(', '):x.artists||''));box.append(art,text);return box;}
function listen(x){return link(x.url,(x.url||'').includes('/results?')?'Tìm bài hát ↗':'Nghe bài hát ↗');}
function badge(text,kind){const n=el('span',text);n.className='badge '+(kind||'');return n;}
function table(headers,rows){const box=el('section'),tools=el('div'),search=el('input'),prev=el('button','← Trước'),next=el('button','Sau →'),count=el('span');tools.className='table-tools';search.placeholder='Tìm trong bảng…';search.setAttribute('aria-label','Tìm trong bảng '+headers[0]);tools.append(search,prev,count,next);const wrap=el('div');wrap.className='table-wrap';const t=el('table'),thead=el('thead'),head=el('tr'),body=el('tbody');headers.forEach(h=>head.append(el('th',h)));thead.append(head);t.append(thead,body);wrap.append(t);box.append(tools,wrap);let page=0;function draw(){const q=search.value.toLocaleLowerCase('vi');const found=rows.filter(row=>row.some(v=>(v instanceof Node?v.textContent:fmt(v)).toLocaleLowerCase('vi').includes(q)));const pages=Math.max(1,Math.ceil(found.length/50));page=Math.min(page,pages-1);body.replaceChildren();for(const row of found.slice(page*50,page*50+50)){const tr=el('tr');for(const v of row){const td=el('td');td.append(v instanceof Node?v:document.createTextNode(fmt(v)));tr.append(td);}body.append(tr);}count.textContent=`${fmt(found.length)} dòng · Trang ${page+1}/${pages}`;prev.disabled=!page;next.disabled=page===pages-1;}search.oninput=()=>{page=0;draw();};prev.onclick=()=>{page--;draw();};next.onclick=()=>{page++;draw();};draw();return box;}
function notice(text){const n=el('p',text);n.className='notice';return n;}
function empty(text){const n=el('p',text);n.className='empty';return n;}
function stats(entries){const box=el('div');box.className='stats-row';for(const [name,value]of entries){const n=el('div');n.className='stat';n.append(el('span',name),el('strong',fmt(value)));box.append(n);}return box;}
function chartName(x){return [platforms[x.source]||x.source,(x.chart_id||'').replaceAll('_',' '),x.country?countryName(x.country):''].filter(Boolean).join(' · ');}
function sparkline(history){const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 80 28');svg.classList.add('sparkline');const vals=(history||[]).slice(-12).map(h=>h.rank).filter(Number.isFinite);if(vals.length<2)return svg;const low=Math.min(...vals),span=Math.max(1,Math.max(...vals)-low);const path=document.createElementNS(ns,'path');path.setAttribute('d',vals.map((v,i)=>(i?'L':'M')+(i*80/(vals.length-1)).toFixed(1)+','+(4+(v-low)/span*20).toFixed(1)).join(' '));if(vals.at(-1)>vals[0])svg.classList.add('down');const title=document.createElementNS(ns,'title');title.textContent='Thứ hạng qua các lần quan sát: '+vals.join(' → ');svg.append(title,path);return svg;}
function panelHead(title,target,sub){const head=el('div');head.className='panel-head';const h=el('h3',title);if(sub)h.append(el('small',' '+sub));const b=el('button','Xem toàn bộ →');b.className='text-link';b.onclick=()=>navigate(target);head.append(h,b);return head;}
async function map(items,onSelect){if(!worldPromise)worldPromise=fetch('/static/world.json').then(r=>{if(!r.ok)throw Error('Chưa tải được bản đồ.');return r.json();});const world=await worldPromise,ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.classList.add('world-map');svg.setAttribute('viewBox','0 0 360 150');svg.setAttribute('role','group');svg.setAttribute('aria-label',`Bản đồ thế giới, ${items.length} quốc gia có dữ liệu`);const byCode=new Map(items.map(x=>[x.country,x]));for(const [code,outline,center]of world){if(code==='AQ')continue;const path=document.createElementNS(ns,'path');path.setAttribute('d',outline||`M${center.join(',')}h.8v.8h-.8Z`);path.classList.add('country');path.dataset.country=code;const data=byCode.get(code);if(data)path.classList.add('has-data');path.setAttribute('tabindex','0');path.setAttribute('role','button');const title=document.createElementNS(ns,'title');title.textContent=countryName(code)+(data?' · '+data.title:' · Chưa có dữ liệu');path.setAttribute('aria-label',title.textContent);path.append(title);path.onclick=()=>{svg.querySelectorAll('.selected').forEach(p=>p.classList.remove('selected'));path.classList.add('selected');onSelect(code,data);};path.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();path.onclick();}};svg.append(path);}return svg;}
function countryPlaceholder(){
 const box=el('div');box.className='country-placeholder';
 box.append(el('span','◎'),el('h3','Chọn một quốc gia'));
 return box;
}
function countryDetail(host,code,item){
 selectedCountry=code;
 const box=el('section'),head=el('header'),identity=el('div'),codeLabel=el('span',code),close=el('button','×');
 box.className='detail-card';head.className='country-detail-head';identity.className='country-identity';codeLabel.className='country-code';
 close.className='country-close';close.setAttribute('aria-label','Đóng chi tiết quốc gia');
 identity.append(codeLabel,el('h3',countryName(code)));head.append(identity,close);box.append(head);
 close.onclick=()=>{selectedCountry=null;host.replaceChildren(countryPlaceholder());host.parentElement.querySelectorAll('.country.selected').forEach(p=>p.classList.remove('selected'));};
 if(!item){box.append(el('p','Chưa có dữ liệu trong khoảng đã chọn.'));host.replaceChildren(box);return;}
 const meta=el('p',`${item.source_count||1} nguồn · ${item.chart_date||'Chưa rõ kỳ'}`);meta.className='country-meta';
 const winner=el('div');winner.className='country-winner';winner.append(el('h4','Bài nổi bật'),song(item),listen(item));
 const sources=el('div');sources.className='source-list';
 (item.sources||[item.source]).filter(Boolean).forEach(s=>sources.append(badge(platforms[s]||s)));
 box.append(meta,winner,sources);
 if(item.charts?.length){
  const more=el('details');more.className='country-charts';more.append(el('summary','Xem từng bảng xếp hạng'));
  for(const chart of item.charts){const row=el('article');row.className='country-source';
   const top=el('div');top.className='country-source-head';top.append(el('b',platforms[chart.source]||chart.source),el('small',chart.date));
   row.append(top,song(chart),link(chart.chart_url||chart.url,'Mở bảng xếp hạng ↗'));more.append(row);
  }
  box.append(more);
 }
 host.replaceChildren(box);
}
async function countryWorkspace(items){
 const workspace=el('section'),mapPanel=el('div'),detail=el('aside'),legend=el('p');
 workspace.className='country-workspace';mapPanel.className='country-map-panel';detail.className='country-detail-slot';
 detail.setAttribute('aria-live','polite');detail.append(countryPlaceholder());legend.className='map-legend';legend.append(el('i'),el('span','Có dữ liệu'));
 const globe=await map(items,(code,item)=>countryDetail(detail,code,item));mapPanel.append(globe,legend);workspace.append(mapPanel,detail);
 if(selectedCountry){const code=selectedCountry;countryDetail(detail,code,items.find(x=>x.country===code));globe.querySelector(`[data-country="${code}"]`)?.classList.add('selected');}
 return workspace;
}
async function home(out,id){
 const p=params(),settled=await Promise.allSettled([api('/api/insights/lifecycle?'+p),api('/api/insights/countries?'+p)]);
 if(id!==requestId)return;out.replaceChildren();
 const grid=el('div'),top=el('section'),list=el('div');grid.className='home-grid';top.className='panel';list.className='top-list';
 top.append(panelHead('Top 5 bài hát','lifecycle','theo hạng trong kỳ'));
 if(settled[0].status==='fulfilled'){
  const seen=new Set(),songs=[...settled[0].value.items].sort((a,b)=>a.best_rank-b.best_rank||String(a.title).localeCompare(String(b.title),'vi')).filter(x=>{
   const k=JSON.stringify([x.title,x.artists]);if(seen.has(k))return false;seen.add(k);return true;
  }).slice(0,5);
  if(!songs.length)list.append(empty('Chưa có thứ hạng trong khoảng đã chọn.'));
  songs.forEach((x,i)=>{const row=el('div'),n=el('span',i<3?['♛','♜','♜'][i]:i+1),rank=el('span','#'+x.best_rank);
   row.className='track-line';n.className='track-number';rank.className='rank-value';rank.title='Hạng tốt nhất quan sát được';
   row.append(n,song(x),rank,sparkline(x.history));list.append(row);
  });
 }else list.append(empty(settled[0].reason.message));
 top.append(list);grid.append(top);
 const globe=el('section');globe.className='panel map-panel';globe.append(panelHead('🌐 Bản đồ nghe nhạc','countries'));
 if(settled[1].status==='fulfilled'){
  const data=settled[1].value,detail=el('div');
  globe.append(await map(data.items,(code,item)=>countryDetail(detail,code,item)));
  const chips=el('div');chips.className='country-card-row';
  data.items.slice(0,3).forEach(x=>{const b=el('button');b.className='country-chip';
   b.append(el('b',countryName(x.country)),el('small',(x.source_count||1)+' nguồn'));
   b.onclick=()=>{selectedCountry=x.country;navigate('countries');};chips.append(b);
  });
  globe.append(chips,detail);$('#coverage').textContent=`${data.items.length} quốc gia · ${(data.sources||[]).length} nguồn`;
 }else globe.append(empty(settled[1].reason.message));
 grid.append(globe);if(id===requestId)out.append(grid);
}
function playlistDetails(x){const d=el('details');d.append(el('summary',`${fmt(x.playlist_count)} playlist · Xem danh sách`));for(const p of x.playlists||[]){const row=el('div');row.className='membership';row.append(link(p.url,p.name+' ↗'),el('small',`${platforms[p.source]||p.source} · ${kinds[p.type]||p.type} · ${p.position?'Vị trí #'+p.position:'Chưa rõ vị trí'}${p.size?' / '+p.size:''}`));if(p.last_seen)row.append(el('small','Thấy gần nhất: '+new Date(p.last_seen).toLocaleString('vi-VN')));d.append(row);}return d;}
function lifecycleTable(data){return table(['Bài hát','Bảng xếp hạng','Hạng tốt nhất','Xu hướng','Lịch sử','Nghe nhạc'],data.items.map(x=>{const h=el('details');h.append(el('summary',`${x.observations} quan sát`));const text=el('p',(x.history||[]).map(r=>(r.period||('Crawl '+(r.crawled_at||'').slice(0,10)))+' · #'+r.rank).join(' → '));text.className='report-meta';h.append(text,el('small',x.reason));return[song(x),chartName(x),'#'+x.best_rank,badge(labels[x.status]||x.status,x.status),h,listen(x)];}));}
function renderReport(host,r){
 host.replaceChildren(stats([['Bình luận trong mẫu',r.sample_size],['Chủ đề',r.topics.length],['Cụm từ',r.keywords.length]]));
 host.append(el('h3','Cụm từ nổi bật'));
 const cloud=el('div');cloud.className='keyword-cloud';
 for(const k of r.keywords){const pill=el('span',k.label);pill.className='keyword-pill';pill.append(el('small',k.count+' bình luận'));pill.title=(k.evidence||[]).map(e=>e.excerpt).join('\n');cloud.append(pill);}
 host.append(cloud);if(!r.keywords.length)host.append(empty('Chưa có cụm từ nổi bật trong mẫu.'));
 const grid=el('div');grid.className='topic-grid';
 for(const t of r.topics){
  const card=el('article');card.className='topic-card';card.append(el('h4',t.label));
  const bar=el('div');bar.className='topic-bar';const fill=el('i');fill.style.width=Math.max(0,Math.min(100,t.share*100))+'%';bar.append(fill);
  card.append(bar,el('small',`${t.count} bình luận · ${(t.share*100).toFixed(1)}% mẫu`));
  if(t.description||t.evidence?.length){const more=el('details');more.className='topic-evidence';more.append(el('summary','Xem bình luận'));
   if(t.description)more.append(el('p',t.description));(t.evidence||[]).forEach(e=>more.append(el('blockquote',e.excerpt)));card.append(more);
  }
  grid.append(card);
 }
 host.append(grid);
}
async function comments(out,id){
 const p=params();p.set('sort',$('#comment-sort').value);const data=await api('/api/comments/tracks?'+p);if(id!==requestId)return;
 out.replaceChildren();$('#status').textContent='';
 if(!data.items.length){out.append(empty('Chưa có bình luận trong khoảng đã chọn.'));return;}
 const form=el('section');form.className='comment-chooser';
 const label=el('label','Bài hát'),select=el('select'),button=el('button','✦ Phân tích bình luận'),report=el('section');
 button.className='report-button';select.setAttribute('aria-label','Chọn bài hát để phân tích bình luận');
 data.items.forEach(x=>select.add(new Option(x.title,x.key)));label.append(select);form.append(label,button);out.append(form,report);
 $('#coverage').textContent=`${data.items.length} bài có bình luận`;select.onchange=()=>report.replaceChildren();
 button.onclick=async()=>{
  button.disabled=true;select.disabled=true;report.replaceChildren(notice('Đang phân tích…'));
  try{
   const response=await fetch('/api/comments/report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({track:select.value,sort:p.get('sort'),days:Number(p.get('days')),as_of:data.as_of})});
   const result=await response.json();if(!response.ok)throw Error(typeof result.detail==='string'?result.detail:'Chưa hoàn tất báo cáo.');
   if(id===requestId)renderReport(report,result);
  }catch(e){report.replaceChildren(empty(e.message));}finally{button.disabled=false;select.disabled=false;}
 };
}
async function load(){
 const id=++requestId;
 $('#heading').textContent=titles[feature][0];$('#description').textContent=titles[feature][1];$('#coverage').textContent='';
 $('#feature-cards').hidden=feature!=='home';$('.comment-filter').hidden=feature!=='comments';
 const out=$('#results');out.replaceChildren(Object.assign(el('div'),{className:'loading-skeleton'}));
 $('#status').textContent='Đang tải dữ liệu…';$('#status').classList.remove('error');
 try{
  if(feature==='home'){await home(out,id);if(id===requestId)$('#status').textContent='';return;}
  if(feature==='comments'){await comments(out,id);return;}
  const data=await api('/api/insights/'+feature+'?'+params());if(id!==requestId)return;
  out.replaceChildren();$('#status').textContent='';
  if(feature==='playlists'){
   out.append(stats([['Bài hát',data.items.length],['Playlist',data.unique_playlists||0]]));
   if(!data.items.length)out.append(empty('Chưa có playlist trong khoảng đã chọn.'));
   else out.append(table(['Hạng','Bài hát','Số playlist','Danh sách playlist','Nghe nhạc'],data.items.map((x,i)=>[i+1,song(x),x.playlist_count,playlistDetails(x),listen(x)])));
  }else if(feature==='countries'){
   const workspace=await countryWorkspace(data.items);if(id!==requestId)return;
   out.append(workspace,table(['Quốc gia','Bài nổi bật','Nguồn','Kỳ gần nhất','Nghe nhạc'],data.items.map(x=>[countryName(x.country),song(x),(x.sources||[x.source]).map(s=>platforms[s]||s).join(', '),x.chart_date,listen(x)])));
   $('#coverage').textContent=`${data.items.length} quốc gia · ${data.sources?.length||0} nguồn`;
  }else{
   out.append(lifecycleTable(data));if(!data.items.length)out.append(empty('Chưa có thứ hạng trong khoảng đã chọn.'));
   $('#coverage').textContent=fmt(data.items.length)+' kết quả';
  }
 }catch(e){if(id!==requestId)return;$('#status').textContent=e.message||'Chưa tải được kết quả.';$('#status').classList.add('error');out.replaceChildren(empty('Chưa đọc được dữ liệu. Vui lòng thử lại.'));}
}
function navigate(next){if(!titles[next])return;feature=next;document.body.dataset.feature=feature;document.querySelectorAll('[data-feature]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.feature===feature)));history.replaceState(null,'','#'+feature);load();}
document.querySelectorAll('[data-feature]').forEach(b=>b.onclick=()=>navigate(b.dataset.feature));document.querySelectorAll('[data-go]').forEach(b=>b.onclick=()=>navigate(b.dataset.go));$('.brand').onclick=e=>{e.preventDefault();navigate('home');};$('#filters').onsubmit=e=>{e.preventDefault();cache.clear();load();};$('#comment-sort').onchange=load;
try{if(localStorage.getItem('music-theme')==='light')document.body.classList.add('light');}catch{}
$('#theme-toggle').onclick=()=>{document.body.classList.toggle('light');try{localStorage.setItem('music-theme',document.body.classList.contains('light')?'light':'dark');}catch{}};$('#help').onclick=()=>$('#help-dialog').showModal();$('#close-help').onclick=()=>$('#help-dialog').close();$('#as-of').value=new Date().toISOString().slice(0,10);navigate(titles[location.hash.slice(1)]?location.hash.slice(1):'home');
