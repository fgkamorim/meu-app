/* ── Theme: aplicar antes de renderizar (evita flash) ── */
(function () {
    var t = localStorage.getItem('fin-theme') || 'dark';
    document.documentElement.setAttribute('data-theme', t);
})();

/* ── Theme Toggle ── */
window.toggleTheme = function () {
    var cur  = document.documentElement.getAttribute('data-theme') || 'dark';
    var next = cur === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('fin-theme', next);
    _updateThemeBtn();
};

function _updateThemeBtn() {
    var btn   = document.getElementById('theme-toggle-btn');
    var theme = document.documentElement.getAttribute('data-theme');
    if (!btn) return;
    btn.textContent = theme === 'dark' ? '☀ Claro' : '🌙 Escuro';
    btn.title = theme === 'dark' ? 'Mudar para modo claro' : 'Mudar para modo escuro';
}

/* ── Sidebar Toggle ── */
function sidebarToggle() {
    var s = document.getElementById('sidebar');
    var o = document.getElementById('sidebar-overlay');
    if (!s) return;
    s.classList.toggle('sidebar-open');
    if (o) o.classList.toggle('active');
    document.body.style.overflow = s.classList.contains('sidebar-open') ? 'hidden' : '';
}

document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
        var s = document.getElementById('sidebar');
        var o = document.getElementById('sidebar-overlay');
        if (s && s.classList.contains('sidebar-open')) {
            s.classList.remove('sidebar-open');
            if (o) o.classList.remove('active');
            document.body.style.overflow = '';
        }
        document.querySelectorAll('.modal-overlay.active').forEach(function (m) {
            m.classList.remove('active');
        });
        document.body.style.overflow = '';
    }
});

/* ── Modal ── */
window.abrirModal = function (id) {
    var el = document.getElementById(id);
    if (el) { el.classList.add('active'); document.body.style.overflow = 'hidden'; }
};
window.fecharModal = function (id) {
    var el = document.getElementById(id);
    if (el) { el.classList.remove('active'); document.body.style.overflow = ''; }
};

/* ── Toast System ── */
function criarToast(msg, cat) {
    var icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
    var cont  = document.getElementById('toast-container');
    if (!cont) return;
    var t = document.createElement('div');
    t.className = 'toast toast-' + (cat || 'success');
    t.innerHTML = '<span class="toast-icon">' + (icons[cat] || '✅') + '</span>' +
                  '<span class="toast-msg">' + msg + '</span>' +
                  '<button class="toast-close" onclick="this.parentElement.remove()" type="button">✕</button>';
    cont.appendChild(t);
    setTimeout(function () {
        t.classList.add('toast-hide');
        setTimeout(function () { if (t.parentElement) t.remove(); }, 380);
    }, 4500);
}

window.mostrarToast = criarToast;

/* ── DOMContentLoaded: injetar botão de tema + flash toasts ── */
document.addEventListener('DOMContentLoaded', function () {

    /* Injetar botão de tema na topbar */
    var topbarActions = document.querySelector('.topbar-actions');
    if (topbarActions) {
        var btn = document.createElement('button');
        btn.id        = 'theme-toggle-btn';
        btn.className = 'topbar-btn topbar-btn-theme';
        btn.onclick   = window.toggleTheme;
        btn.type      = 'button';
        _updateThemeBtn();
        btn.textContent = document.documentElement.getAttribute('data-theme') === 'dark' ? '☀ Claro' : '🌙 Escuro';
        topbarActions.insertBefore(btn, topbarActions.firstChild);
    }

    /* Flash toasts */
    var fd = document.getElementById('flash-data');
    if (fd) {
        fd.querySelectorAll('span[data-msg]').forEach(function (el) {
            criarToast(el.dataset.msg, el.dataset.cat);
        });
    }
});

/* ── Visual Dashboard: filtro de meses ── */
document.addEventListener('DOMContentLoaded', function () {
    var btns = document.querySelectorAll('.vd-filter-btn');
    btns.forEach(function (btn) {
        btn.addEventListener('click', function () {
            btns.forEach(function (b) { b.classList.remove('ativo'); });
            btn.classList.add('ativo');
            var meses = parseInt(btn.dataset.meses, 10);
            document.querySelectorAll('.vd-month-col').forEach(function (col, idx, all) {
                if (meses === 0) {
                    col.style.display = '';
                } else {
                    col.style.display = (idx >= all.length - meses) ? '' : 'none';
                }
            });
        });
    });

    /* Disparar o filtro default (3 meses) */
    var def = document.querySelector('.vd-filter-btn.ativo');
    if (def) def.click();
});
