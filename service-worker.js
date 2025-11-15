// service-worker.js v4
const CACHE_NAME = 'atom-v4'; // Updated cache name
const urlsToCache = [
  '/',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/style.css',
  // Expanded: Cache common dynamic routes
  '/?league=PL',  // Example: Premier League
  '/?league=CL',  // Champions League
  '/?team=33',    // Example team ID
  // Add more based on popular leagues/teams
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