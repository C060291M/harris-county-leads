const fs = require('fs');
let d = fs.readFileSync('C:/Users/cmuno/OneDrive/Desktop/underwriteiq/apps/underwriteiq/dashboard.html','utf8');

// Add filter buttons after the search input
const searchInput = `<input type="text" id="crm-search" placeholder="Search by name, address, or phone..." oninput="renderCRM()" style="width:100%;padding:9px 14px;border:0.5px solid var(--border);border-radius:8px;font-size:13px;background:var(--card);color:var(--text)"/>
      </div>`;

const searchWithFilters = `<input type="text" id="crm-search" placeholder="Search by name, address, or phone..." oninput="renderCRM()" style="width:100%;padding:9px 14px;border:0.5px solid var(--border);border-radius:8px;font-size:13px;background:var(--card);color:var(--text)"/>
      </div>
      <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
        <span style="font-size:11px;font-weight:500;color:var(--muted)">Filter:</span>
        <button id="btn-skip-traced" onclick="toggleFilter('skipTraced')" style="padding:5px 12px;border:0.5px solid var(--border);border-radius:6px;font-size:12px;background:var(--card);color:var(--text);cursor:pointer">Skip Traced</button>
        <button id="btn-not-skip-traced" onclick="toggleFilter('notSkipTraced')" style="padding:5px 12px;border:0.5px solid var(--border);border-radius:6px;font-size:12px;background:var(--card);color:var(--text);cursor:pointer">Not Skip Traced</button>
        <button id="btn-newest" onclick="toggleFilter('newest')" style="padding:5px 12px;border:0.5px solid var(--border);border-radius:6px;font-size:12px;background:var(--card);color:var(--text);cursor:pointer">Newest First</button>
        <button id="btn-oldest" onclick="toggleFilter('oldest')" style="padding:5px 12px;border:0.5px solid var(--border);border-radius:6px;font-size:12px;background:var(--card);color:var(--text);cursor:pointer">Oldest First</button>
        <button onclick="clearFilters()" style="padding:5px 12px;border:0.5px solid var(--border);border-radius:6px;font-size:12px;background:var(--card);color:var(--muted);cursor:pointer">Clear</button>
      </div>`;

d = d.replace(searchInput, searchWithFilters);

// Add filter logic to the JS
const renderCRMFunc = `function renderCRM(){`;
const filterLogic = `let crmFilter = '';
function toggleFilter(f){
  crmFilter = crmFilter === f ? '' : f;
  document.querySelectorAll('[id^="btn-"]').forEach(b=>b.style.background='var(--card)');
  if(crmFilter){
    const btn = document.getElementById('btn-'+crmFilter.replace(/([A-Z])/g,'-$1').toLowerCase().replace(/^-/,''));
    if(btn) btn.style.background='var(--accent)';
  }
  renderCRM();
}
function clearFilters(){ crmFilter=''; document.querySelectorAll('[id^="btn-"]').forEach(b=>b.style.background='var(--card)'); renderCRM(); }
function renderCRM(){`;

d = d.replace(renderCRMFunc, filterLogic);

// Add filter application inside renderCRM after crmLeads is filtered
d = d.replace(
  'let filtered = crmLeads.filter(l => {',
  `let filtered = crmLeads.filter(l => {
    if(crmFilter==='skipTraced' && !l.phone && !l.email) return false;
    if(crmFilter==='notSkipTraced' && (l.phone || l.email)) return false;`
);

// Add sort logic
d = d.replace(
  'filtered.sort(',
  `if(crmFilter==='newest') filtered.sort((a,b)=>new Date(b.added_at||0)-new Date(a.added_at||0));
  else if(crmFilter==='oldest') filtered.sort((a,b)=>new Date(a.added_at||0)-new Date(b.added_at||0));
  else filtered.sort(`
);

fs.writeFileSync('C:/Users/cmuno/OneDrive/Desktop/underwriteiq/apps/underwriteiq/dashboard.html', d, 'utf8');
console.log('Done.');
