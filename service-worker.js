// service-worker.js v3
const CACHE_NAME = 'atom-v3'; // <-- Updated cache name
const urlsToCache = [
  '/',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/style.css',
  // Add more if you have dynamic routes, e.g., '/?league=PL'
];

// Install: Cache assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
  );
  self.skipWaiting(); // Activate new SW immediately
});

// Activate: Clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.filter(name => name !== CACHE_NAME)
          .map(name => caches.delete(name))
      );
    })
  );
  self.clients.claim(); // Take control of clients
});

// Fetch: Cache-then-network, fallback to offline page
self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request)
      .then(response => {
        if (response) return response;
        return fetch(event.request)
          .then(networkResponse => {
            if (event.request.method === 'GET') {
              const clone = networkResponse.clone();
              caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
            }
            return networkResponse;
          })
          .catch(() => {
            // Offline fallback: Custom offline page or cached '/'
            return caches.match('/') || new Response('Offline - Check connection.', { status: 200, headers: { 'Content-Type': 'text/plain' } });
          });
      })
  );
});