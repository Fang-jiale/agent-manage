// YwMatrix PWA Service Worker
// 职责仅限：让站点可安装（添加到主屏）+ 壳页面离线兜底。
// 业务数据一律走网络（WS/API 不缓存）；壳资源也是网络优先——
// 在线时永远拿最新（内部工具迭代频繁），断网时才回退缓存。
const SHELL_CACHE = 'ywm-shell-v2';
const SHELL_ASSETS = [
    '/static/shared.css',
    '/static/index.css',
    '/static/logo.svg',
    '/static/icon-192.png'
];

self.addEventListener('install', (e) => {
    e.waitUntil(caches.open(SHELL_CACHE).then((c) => c.addAll(SHELL_ASSETS)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (e) => {
    e.waitUntil(
        caches.keys()
            .then((keys) => Promise.all(keys.filter((k) => k !== SHELL_CACHE).map((k) => caches.delete(k))))
            .then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', (e) => {
    const url = new URL(e.request.url);
    if (e.request.method !== 'GET' || url.pathname.startsWith('/ws/') || url.pathname.startsWith('/auth/')) return;
    if (SHELL_ASSETS.some((a) => url.pathname === a)) {
        e.respondWith(
            fetch(e.request)
                .then((res) => {
                    const copy = res.clone();
                    caches.open(SHELL_CACHE).then((c) => c.put(e.request, copy));
                    return res;
                })
                .catch(() => caches.match(e.request))
        );
    }
});
