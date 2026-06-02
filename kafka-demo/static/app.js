// ── Priority selector ────────────────────────────────────────────────────────
document.querySelectorAll('.priority-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.priority-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('priority').value = btn.dataset.v;
  });
});

// ── Validation ───────────────────────────────────────────────────────────────
function setErr(fieldId, errId, show) {
  const field = document.getElementById(fieldId);
  const err   = document.getElementById(errId);
  if (show) { field.classList.add('invalid'); err.style.display = 'block'; }
  else       { field.classList.remove('invalid'); err.style.display = 'none'; }
}

function validate() {
  let ok = true;
  const name     = document.getElementById('name').value.trim();
  const email    = document.getElementById('email').value.trim();
  const category = document.getElementById('category').value;
  const message  = document.getElementById('message').value.trim();

  setErr('name',     'nameErr',     !name);
  setErr('email',    'emailErr',    !email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email));
  setErr('category', 'categoryErr', !category);
  setErr('message',  'messageErr',  !message);

  if (!name || !email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || !category || !message) ok = false;
  return ok;
}

// ── Toast helper ─────────────────────────────────────────────────────────────
function showToast(msg, isError = false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show' + (isError ? ' error' : '');
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.classList.remove('show'), 4000);
}

// ── Submit ───────────────────────────────────────────────────────────────────
async function submitForm() {
  if (!validate()) return;

  const btn = document.getElementById('submitBtn');
  btn.disabled = true;
  btn.classList.add('loading');

  const payload = {
    name:     document.getElementById('name').value.trim(),
    email:    document.getElementById('email').value.trim(),
    category: document.getElementById('category').value,
    priority: document.getElementById('priority').value,
    message:  document.getElementById('message').value.trim(),
  };

  try {
    const res  = await fetch('/submit', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || 'Server error');

    showToast(`✓ Published  id: ${data.message_id.slice(0,8)}…`);

    // Reset form
    document.getElementById('name').value    = '';
    document.getElementById('email').value   = '';
    document.getElementById('category').value = '';
    document.getElementById('message').value = '';

    // Refresh feed after a brief delay (consumer needs a moment)
    setTimeout(loadSubmissions, 1200);

  } catch (err) {
    showToast('✗ ' + err.message, true);
  } finally {
    btn.disabled = false;
    btn.classList.remove('loading');
  }
}

// ── Load submissions ─────────────────────────────────────────────────────────
function timeAgo(isoStr) {
  const diff = Date.now() - new Date(isoStr).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60)  return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s/60)}m ago`;
  return `${Math.floor(s/3600)}h ago`;
}

let _prevTotal = 0;

async function loadSubmissions() {
  try {
    const res  = await fetch('/submissions?limit=30');
    const data = await res.json();
    const list = document.getElementById('feedList');

    document.getElementById('totalCount').textContent = data.total;

    if (!data.submissions || data.submissions.length === 0) {
      list.innerHTML = `
        <div class="empty-state">
          <div class="empty-icon">📭</div>
          <p>No submissions yet.<br>Submit data to see it appear here.</p>
        </div>`;
      _prevTotal = 0;
      return;
    }

    const isNew = data.total > _prevTotal;
    _prevTotal = data.total;

    list.innerHTML = data.submissions.map((s, i) => `
      <div class="submission-card ${i === 0 && isNew ? 'new' : ''}">
        <div class="card-priority ${s.priority || 'normal'}"></div>
        <div class="card-body">
          <div class="card-name">${esc(s.name)}</div>
          <div class="card-email">${esc(s.email)}</div>
          <div class="card-message">${esc(s.message)}</div>
        </div>
        <div class="card-meta">
          <span class="card-category">${esc(s.category)}</span>
          <div class="card-time">${timeAgo(s.submitted_at)}</div>
        </div>
      </div>
    `).join('');

  } catch (err) {
    console.error('Failed to load submissions:', err);
  }
}

function esc(str) {
  return String(str ?? '')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;');
}

// ── Health check ─────────────────────────────────────────────────────────────
async function checkHealth() {
  try {
    const res  = await fetch('/health');
    const data = await res.json();

    const kEl = document.getElementById('kafkaStatus');
    const mEl = document.getElementById('mongoStatus');

    const kafkaOk = data.kafka === 'ok';
    const mongoOk = data.mongodb === 'ok';

    kEl.innerHTML = `<span class="status-dot ${kafkaOk ? 'ok' : 'err'}"></span>kafka: ${kafkaOk ? 'connected' : 'error'}`;
    mEl.innerHTML = `<span class="status-dot ${mongoOk ? 'ok' : 'err'}"></span>mongo: ${mongoOk ? 'connected' : 'error'}`;

  } catch {
    document.getElementById('kafkaStatus').innerHTML = `<span class="status-dot err"></span>kafka: unreachable`;
    document.getElementById('mongoStatus').innerHTML = `<span class="status-dot err"></span>mongo: unreachable`;
  }
}

// ── Init ─────────────────────────────────────────────────────────────────────
checkHealth();
loadSubmissions();

// Auto-refresh feed every 5s
setInterval(loadSubmissions, 5000);
// Re-check health every 30s
setInterval(checkHealth, 30000);

// Enter key submits form
document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && e.ctrlKey) submitForm();
});
