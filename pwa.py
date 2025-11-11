# pwa.py v1.0
import streamlit as st
from pathlib import Path

# Conditional import for Server
try:
    from streamlit.web.server.server import Server
    STREAMLIT_SERVER_AVAILABLE = True
except ImportError:
    STREAMLIT_SERVER_AVAILABLE = False


def get_file(path: str) -> bytes:
    """Reads file content for PWA serving."""
    p = Path(__file__).parent / path
    return p.read_bytes() if p.exists() else b""


def add_file_route(path: str, content_factory: callable, content_type: str):
    """Adds a file route to the Streamlit server for PWA files."""
    if not STREAMLIT_SERVER_AVAILABLE:
        return  # Do nothing if Server is not available

    try:
        routes = Server.get_current()._runtime._routes
        if path not in routes:
            Server.get_current()._add_file_route(path, content_factory, content_type)
    except Exception:
        # Suppress errors if Server is not available
        pass


def inject_pwa():
    """Injects the necessary PWA links and service worker registration script."""

    # Register file routes
    for file, ctype in [
        ("manifest.json", "application/json"),
        ("service-worker.js", "application/javascript"),
        ("static/icon-192.png", "image/png"),
        ("static/icon-512.png", "image/png"),
        ("static/style.css", "text/css"),
    ]:
        key = f"serve_{file.replace('/', '_')}"
        if key not in st.session_state:
            # Use a lambda to capture file and ctype correctly
            @st.cache_data
            def serve(f=file, c=ctype):
                return get_file(f), c

            add_file_route(file, lambda f=file, c=ctype: serve(f, c), ctype)
            st.session_state[key] = True

    # Inject HTML links
    manifest = """
    <link rel="manifest" href="/manifest.json">
    <link rel="apple-touch-icon" href="/static/icon-192.png">
    <meta name="theme-color" content="#ff4b4b">
    <link rel="stylesheet" href="/static/style.css">
    <script>
      if ('serviceWorker' in navigator) {
        window.addEventListener('load', () => {
          navigator.serviceWorker.register('/service-worker.js');
        });
      }
    </script>
    """
    st.markdown(manifest, unsafe_allow_html=True)