#!/usr/bin/env python3
"""
Live server for Missed Pax Dashboard.

Usage:
    python dashboard_server.py

Then open http://localhost:8050 in your browser.
Click the 'Refresh Data' button on the dashboard to pull latest records from DB.
"""
import sys, os, threading, webbrowser
from datetime import datetime, timezone, timedelta

# Must be set before importing missed_pax_dashboard so it doesn't auto-run
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, send_file, jsonify, send_from_directory
import missed_pax_dashboard as mpd

PORT       = int(os.environ.get('PORT', 8050))
FROM_DATE  = '2026-03-02'   # fixed start date for the dashboard
IST        = mpd.IST

app = Flask(__name__)
_lock         = threading.Lock()
_refreshing   = False
_startup_error = None


def _build_range():
    start_dt = datetime.strptime(FROM_DATE, '%Y-%m-%d').replace(tzinfo=IST)
    end_dt   = datetime.now(IST).replace(hour=23, minute=59, second=59, microsecond=0)
    return (
        start_dt.astimezone(timezone.utc),
        end_dt.astimezone(timezone.utc),
        start_dt.strftime('%d %b %Y'),
        end_dt.strftime('%d %b %Y'),
    )


def _do_refresh():
    env    = mpd.load_env()
    db_url = env.get('DATABASE_URL') or os.environ.get('DATABASE_URL')
    if not db_url:
        raise RuntimeError('DATABASE_URL not set')
    start_utc, end_utc, start_label, end_label = _build_range()
    mpd.run_once(db_url, start_utc, end_utc, start_label, end_label)


BASE = os.path.dirname(os.path.abspath(__file__))

@app.route('/chart.umd.min.js')
def chart_js():
    return send_from_directory(BASE, 'chart.umd.min.js')

@app.route('/')
def index():
    if not os.path.exists(mpd.OUT_FILE):
        if _startup_error:
            return f'''<html><head><meta http-equiv="refresh" content="30"></head>
<body style="background:#0f1117;color:#eee;font-family:sans-serif;padding:60px;text-align:center">
<h2 style="color:#ef5350">&#9888; Dashboard failed to load</h2>
<p style="color:#aaa;max-width:600px;margin:auto">{_startup_error}</p>
<p style="color:#555;margin-top:20px">Click <b>Refresh Data</b> button once the issue is resolved, or wait — page retries in 30s.</p>
</body></html>'''
        return '''<html><head><meta http-equiv="refresh" content="5"></head>
<body style="background:#0f1117;color:#eee;font-family:sans-serif;padding:60px;text-align:center">
<h2 style="color:#4fc3f7">&#8635; Loading dashboard data...</h2>
<p style="color:#555">Fetching records from database. This page will refresh automatically.</p>
</body></html>'''
    return send_file(mpd.OUT_FILE)


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    global _refreshing
    if _refreshing:
        return jsonify({'status': 'busy', 'msg': 'Refresh already in progress — please wait'}), 429

    if not _lock.acquire(blocking=False):
        return jsonify({'status': 'busy', 'msg': 'Refresh already in progress'}), 429

    _refreshing = True
    try:
        _do_refresh()
        _refreshing = False
        _lock.release()
        now_ist = datetime.now(IST).strftime('%d %b %Y %I:%M %p')
        return jsonify({'status': 'ok', 'last_updated': now_ist})
    except Exception as e:
        _refreshing = False
        _lock.release()
        print(f'  ERROR during refresh: {e}')
        return jsonify({'status': 'error', 'msg': str(e)}), 500


@app.route('/api/status')
def api_status():
    return jsonify({
        'status':      'ok',
        'refreshing':  _refreshing,
        'server_time': datetime.now(IST).strftime('%d %b %Y %I:%M %p'),
    })


def _background_startup():
    global _startup_error
    try:
        _do_refresh()
        _startup_error = None
        print('  Initial data fetch complete.')
    except Exception as e:
        _startup_error = str(e)
        print(f'  WARNING: Initial fetch failed: {e}')

if __name__ == '__main__':
    # Start refresh in background so Flask binds the port immediately
    threading.Thread(target=_background_startup, daemon=True).start()

    is_local = PORT == 8050 and not os.environ.get('RENDER') and not os.environ.get('RAILWAY_ENVIRONMENT')
    if is_local:
        threading.Timer(3.0, lambda: webbrowser.open(f'http://localhost:{PORT}')).start()

    print(f'  Missed Pax Dashboard — Live Server on port {PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False, threaded=True)
