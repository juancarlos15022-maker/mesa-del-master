'use strict';
/* Mesa del Máster · versión app: Supabase para los datos y la API de Anthropic (clave de cada jugador) para el máster. */
window.STANDALONE = true;
const SB_URL = 'https://rkvsiegttcqjblndzbmu.supabase.co';
const SB_KEY = 'sb_publishable_LrMlLSEjI3x5-6riKCOc4g_TLWZGTn-';
const SB_ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJrdnNpZWd0dGNxamJsbmR6Ym11Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTM4NDIyMjAsImV4cCI6MjEwOTQxODIyMH0.6TE_BfNvU0-FCnmq_EX24o0Pqnqx9k2FLzkDxSHc9W4';
const sb = window.supabase ? window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: true, autoRefreshToken: true } }) : null;
const LS = { get(k){ try { return localStorage.getItem(k) || ''; } catch(e){ return ''; } }, set(k,v){ try { v ? localStorage.setItem(k,v) : localStorage.removeItem(k); } catch(e){} } };
const MODELS = [['claude-sonnet-5-5','Sonnet 5.5 · equilibrado (recomendado)'],['claude-opus-5-5','Opus 5.5 · el mejor narrador, más caro'],['claude-haiku-5-5','Haiku 5.5 · rápido y barato']];

/* ---------- Base de datos (misma interfaz que usa el juego) ---------- */
const dbErr = e => ({ code: (e && (e.code === '42501' || e.code === 'PGRST301')) ? 'invalid_argument' : (e && e.code === 'P0002') ? 'invalid_argument' : 'unavailable', message: (e && e.message) || 'error' });
const snapOf = (row, path) => ({ id: path.split('/').pop(), exists: !!row, data: () => row ? row.data : undefined, metadata: { fromCache: false, hasPendingWrites: false } });
const rid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
function watch(filterCol, value, onChange){
  let timer = null; const fire = () => { clearTimeout(timer); timer = setTimeout(onChange, 120); };
  const ch = sb.channel('w' + rid())
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'docs', filter: filterCol + '=eq.' + value }, fire)
    .on('postgres_changes', { event: 'UPDATE', schema: 'public', table: 'docs', filter: filterCol + '=eq.' + value }, fire)
    .on('postgres_changes', { event: 'DELETE', schema: 'public', table: 'docs' }, p => { const o = p.old || {}; if (o[filterCol] === value || o[filterCol] === undefined) fire(); })
    .subscribe();
  const poll = setInterval(onChange, 30000);
  return () => { clearTimeout(timer); clearInterval(poll); sb.removeChannel(ch); };
}
function docRef(path){
  const coll = path.split('/').slice(0, -1).join('/');
  const ref = {
    id: path.split('/').pop(), path,
    async get(){ const { data, error } = await sb.from('docs').select('data').eq('path', path).maybeSingle(); if (error) throw dbErr(error); return snapOf(data, path); },
    async set(d){ const { error } = await sb.from('docs').upsert({ path, coll, data: d, updated_at: new Date().toISOString() }); if (error) throw dbErr(error); },
    async update(d){ const { error } = await sb.rpc('doc_update', { p_path: path, p_patch: d }); if (error) throw dbErr(error); },
    async delete(){ const { error } = await sb.from('docs').delete().eq('path', path); if (error) throw dbErr(error); },
    async acquire(o){ const { data, error } = await sb.rpc('doc_acquire', { p_path: path, p_holder: String(o.holder), p_ttl: o.ttlMs || 30000 }); return { acquired: error ? true : !!data }; },
    onSnapshot(next, onError){ let alive = true; const load = async () => { try { const s = await ref.get(); if (alive) next(s); } catch(e){ if (alive && onError) onError(e); } }; load(); const stop = watch('path', path, load); return () => { alive = false; stop(); }; },
    collection(sub){ return collRef(path + '/' + sub); }
  };
  return ref;
}
function collRef(cpath, q){
  q = q || { w: [], o: null, l: null };
  const run = async () => {
    let r = sb.from('docs').select('path,data').eq('coll', cpath);
    for (const [f, op, v] of q.w){ const col = 'data->>' + f; const sv = String(v);
      if (op === '==') r = r.eq(col, sv); else if (op === '!=') r = r.neq(col, sv); else if (op === '>') r = r.gt(col, sv); else if (op === '<') r = r.lt(col, sv); else if (op === '>=') r = r.gte(col, sv); else if (op === '<=') r = r.lte(col, sv); }
    if (q.o) r = r.order('data->' + q.o[0], { ascending: q.o[1] !== 'desc', nullsFirst: false });
    r = r.limit(q.l || 1000);
    const { data, error } = await r; if (error) throw dbErr(error);
    const docs = (data || []).map(row => snapOf(row, row.path));
    return { docs, size: docs.length, empty: !docs.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: false } };
  };
  const ref = {
    path: cpath,
    doc(id){ return docRef(cpath + '/' + (id || rid())); },
    async add(d){ const r = ref.doc(); await r.set(d); return r; },
    where(f, op, v){ return collRef(cpath, { ...q, w: [...q.w, [f, op, v]] }); },
    orderBy(f, dir){ return collRef(cpath, { ...q, o: [f, dir || 'asc'] }); },
    limit(n){ return collRef(cpath, { ...q, l: n }); },
    get: run,
    onSnapshot(next, onError){ let alive = true; const load = async () => { try { const s = await run(); if (alive) next(s); } catch(e){ if (alive && onError) onError(e); } }; load(); const stop = watch('coll', cpath, load); return () => { alive = false; stop(); }; }
  };
  return ref;
}
const dbAdapter = { doc: docRef, collection: p => collRef(p) };

/* ---------- Jugador ---------- */
let currentUser = null;
const userAdapter = {
  async id(){ return currentUser ? currentUser.id : null; },
  async isOwner(){ return false; }, async canEdit(){ return true; }, async can(){ return true; },
  async me(){ return { id: currentUser ? currentUser.id : null, name: currentUser ? (currentUser.email || '').split('@')[0] : '', email: currentUser ? currentUser.email : null, avatarUrl: '', color: '#e8ad5d', isOwner: false, canEdit: true }; },
  async profiles(ids){ const o = {}; [].concat(ids).forEach(i => o[i] = { id: i, name: '', avatarUrl: '', color: '#e8ad5d', email: null, isMe: currentUser && i === currentUser.id, guest: false }); return o; }
};

/* ---------- Máster IA con la clave de cada jugador ---------- */
async function sampleAdapter(input, opts){
  opts = opts || {};
  const key = LS.get('mdm_key');
  if (!key){ setTimeout(settingsModal, 50); throw { code: 'not_granted', message: 'Falta la clave de Anthropic' }; }
  const messages = typeof input === 'string' ? [{ role: 'user', content: input }] : input;
  let text = '', truncated = false, res;
  try {
    res = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST', signal: opts.signal, headers: { 'content-type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01', 'anthropic-dangerous-direct-browser-access': 'true' }, body: JSON.stringify({ model: LS.get('mdm_model') || MODELS[0][0], max_tokens: 8000, stream: true, messages }) });
  } catch(e){ if (e && e.name === 'AbortError') throw { code: 'cancelled', message: 'cancelado' }; throw { code: 'upstream_error', message: String(e) }; }
  if (!res.ok){
    const t = await res.text().catch(() => '');
    if (res.status === 401 || res.status === 403){ toast('Tu clave de Anthropic no es válida. Revísala en Ajustes.'); throw { code: 'not_granted', message: t }; }
    if (res.status === 429 || res.status === 529) throw { code: 'rate_limited', message: t };
    if (res.status === 400 && /too long|too many tokens/i.test(t)) throw { code: 'prompt_too_large', message: t };
    if (res.status === 400 && /credit|billing/i.test(t)){ toast('Tu cuenta de Anthropic no tiene saldo. Añade crédito en console.anthropic.com.'); throw { code: 'not_granted', message: t }; }
    if (res.status === 404 || /model/i.test(t)){ toast('El modelo elegido no está disponible para tu clave. Cambia de modelo en Ajustes.'); }
    throw { code: 'upstream_error', message: t };
  }
  const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = '';
  try {
    for (;;){
      const { value, done } = await reader.read(); if (done) break;
      buf += dec.decode(value, { stream: true });
      let i; while ((i = buf.indexOf('\n')) >= 0){
        const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1);
        if (!line.startsWith('data:')) continue;
        let ev; try { ev = JSON.parse(line.slice(5)); } catch(e){ continue; }
        if (ev.type === 'content_block_delta' && ev.delta && ev.delta.type === 'text_delta'){ text += ev.delta.text; if (opts.onText) try { opts.onText({ text, delta: ev.delta.text }); } catch(e){} }
        else if (ev.type === 'message_delta' && ev.delta && ev.delta.stop_reason === 'max_tokens') truncated = true;
        else if (ev.type === 'error') throw { code: 'upstream_error', message: JSON.stringify(ev.error || ev), text };
      }
    }
  } catch(e){ if (e && e.name === 'AbortError') throw { code: 'cancelled', message: 'cancelado', text }; if (e && e.code) throw e; throw { code: 'upstream_error', message: String(e), text }; }
  if (!text.trim()) throw { code: 'empty_completion', message: 'sin texto' };
  return { text, truncated, modelTierApplied: 'default' };
}
sampleAdapter.json = async (input, opts) => {
  const extra = '\n\nResponde solo con el JSON pedido, sin texto antes ni después.';
  const inp = typeof input === 'string' ? input + extra : input;
  const r = await sampleAdapter(inp, opts); const t = r.text;
  const tries = [t, (t.match(/```(?:json)?\s*([\s\S]*?)```/) || [])[1]];
  const a = Math.min(...['{', '['].map(c => t.indexOf(c)).filter(x => x >= 0)); const b = Math.max(t.lastIndexOf('}'), t.lastIndexOf(']'));
  if (a >= 0 && b > a) tries.push(t.slice(a, b + 1));
  for (const x of tries){ if (!x) continue; try { return JSON.parse(x); } catch(e){} }
  throw { code: 'invalid_json', message: 'JSON no válido', text: t };
};
sampleAdapter.limits = async () => ({ maxPromptBytes: 900000 });

/* ---------- Ajustes, acceso e instalación ---------- */
function settingsModal(){
  modal(close => {
    const k = h('input', { id: 'set-key', type: 'password', autocomplete: 'off', placeholder: 'sk-ant-…', value: LS.get('mdm_key') });
    const m = h('select', { id: 'set-model' }, MODELS.map(([v, l]) => h('option', { value: v, selected: (LS.get('mdm_model') || MODELS[0][0]) === v ? '' : null }, l)));
    return [h('h2', null, 'Ajustes'),
      h('label', { class: 'label', for: 'set-key' }, 'Tu clave de la API de Anthropic'), k,
      h('p', { class: 'muted', style: 'font-size:14px' }, 'El máster usa tu propia clave y cada turno se cobra en tu cuenta de Anthropic. Se guarda solo en este dispositivo. Consíguela en ', h('a', { href: 'https://console.anthropic.com/settings/keys', target: '_blank', rel: 'noopener', style: 'color:var(--candle)' }, 'console.anthropic.com'), '.'),
      h('label', { class: 'label', for: 'set-model' }, 'Modelo del máster'), m,
      h('p', { class: 'faint', style: 'font-size:13px' }, 'Sesión: ' + (currentUser ? currentUser.email : '—')),
      h('div', { class: 'row' }, h('button', { class: 'primary', onclick: () => { LS.set('mdm_key', k.value.trim()); LS.set('mdm_model', m.value); close(); toast('Ajustes guardados'); } }, 'Guardar'),
        h('button', { class: 'ghost', onclick: close }, 'Cerrar'), h('span', { class: 'grow' }),
        h('button', { class: 'danger small', onclick: async () => { close(); await sb.auth.signOut(); location.hash = ''; location.reload(); } }, 'Cerrar sesión'))];
  });
}
let installPrompt = null;
window.addEventListener('beforeinstallprompt', e => { e.preventDefault(); installPrompt = e; const b = document.getElementById('install-btn'); if (b) b.hidden = false; });
function headerExtras(){
  const wrap = h('div', { class: 'row', style: 'gap:6px;margin-left:auto' });
  const ib = h('button', { class: 'small ghost', id: 'install-btn', hidden: installPrompt ? null : '', onclick: async () => { if (!installPrompt) return; installPrompt.prompt(); await installPrompt.userChoice; installPrompt = null; ib.hidden = true; } }, 'Instalar app');
  wrap.append(ib, h('button', { class: 'small', onclick: settingsModal }, LS.get('mdm_key') ? 'Ajustes' : 'Ajustes · falta clave'));
  return wrap;
}
function authView(){
  let mode = 'in';
  const box = h('div', { class: 'hero', style: 'max-width:440px' });
  const draw = () => {
    const email = h('input', { id: 'au-mail', type: 'email', autocomplete: 'email', placeholder: 'tu@correo.com' });
    const pass = h('input', { id: 'au-pass', type: 'password', autocomplete: mode === 'in' ? 'current-password' : 'new-password', placeholder: mode === 'in' ? 'Contraseña' : 'Mínimo 6 caracteres' });
    const msg = h('p', { class: 'muted', style: 'font-size:15px', role: 'status' });
    const go = async e => { e.preventDefault(); msg.textContent = mode === 'in' ? 'Entrando…' : 'Creando tu cuenta…';
      const fn = mode === 'in' ? sb.auth.signInWithPassword({ email: email.value.trim(), password: pass.value }) : sb.auth.signUp({ email: email.value.trim(), password: pass.value, options: { emailRedirectTo: location.origin + location.pathname } });
      const { data, error } = await fn;
      if (error){ msg.textContent = /invalid login/i.test(error.message) ? 'Correo o contraseña incorrectos.' : /already registered/i.test(error.message) ? 'Ese correo ya tiene cuenta: entra con tu contraseña.' : /password/i.test(error.message) ? 'La contraseña debe tener al menos 6 caracteres.' : 'No se pudo: ' + error.message; return; }
      if (mode === 'up' && !data.session) msg.textContent = 'Cuenta creada. Revisa tu correo y pulsa el enlace de confirmación; después entra aquí.';
    };
    box.replaceChildren(
      h('span', { class: 'label' }, 'Rol de mesa · reglas 5e'), h('h1', null, 'Mesa del Máster'),
      h('p', null, 'Una campaña sin final con un máster IA. Entra con tu cuenta para seguir tu historia en cualquier dispositivo.'),
      h('form', { class: 'stack', onsubmit: go }, h('label', { class: 'label', for: 'au-mail' }, 'Correo'), email, h('label', { class: 'label', for: 'au-pass' }, 'Contraseña'), pass,
        h('button', { class: 'primary', type: 'submit' }, mode === 'in' ? 'Entrar' : 'Crear cuenta'), msg),
      h('button', { class: 'ghost', onclick: () => { mode = mode === 'in' ? 'up' : 'in'; draw(); } }, mode === 'in' ? '¿No tienes cuenta? Créala' : 'Ya tengo cuenta: entrar'));
  };
  draw(); return box;
}
let booted = false;
async function startGame(session){
  currentUser = session.user;
  if (booted) return; booted = true;
  S.db = dbAdapter; S.user = userAdapter; S.sample = sampleAdapter; S.me = currentUser.id; S.canWrite = true; S.ready = true; S.offline = false;
  subscribeLobby();
  window.addEventListener('hashchange', route); route();
  if (!LS.get('mdm_key')) setTimeout(() => toast('Añade tu clave de Anthropic en Ajustes para que el máster pueda narrar.'), 800);
}
async function standaloneBoot(){
  S.ready = true; S.offline = false;
  if (!sb){ document.getElementById('app').replaceChildren(h('div', { class: 'hero' }, h('h1', null, 'Sin conexión'), h('p', null, 'No se pudo cargar la Mesa del Máster. Revisa tu conexión a internet y vuelve a abrir la app.'))); return; }
  const { data } = await sb.auth.getSession();
  if (data.session) return startGame(data.session);
  const app = document.getElementById('app'); app.replaceChildren(authView());
  sb.auth.onAuthStateChange((ev, session) => { if (session && !booted){ startGame(session); render(); } });
}
window.notifyDiscord = async (campana, texto, turno, tipo) => {
  try { const { data } = await sb.auth.getSession(); const tk = data.session && data.session.access_token; if (!tk) return;
    await fetch(SB_URL + '/functions/v1/aviso', { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + tk, apikey: SB_KEY }, body: JSON.stringify({ campana, texto, turno, tipo: tipo || 'turno' }) });
  } catch(e){}
};
if ('serviceWorker' in navigator) window.addEventListener('load', () => navigator.serviceWorker.register('sw.js').catch(() => {}));
