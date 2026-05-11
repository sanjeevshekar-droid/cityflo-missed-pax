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
_lock       = threading.Lock()
_refreshing = False


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
    db_url = env.get('DATABASE_URL')
    if not db_url:
        raise RuntimeError('DATABASE_URL not found in env file')
    start_utc, end_utc, start_label, end_label = _build_range()
    mpd.run_once(db_url, start_utc, end_utc, start_label, end_label)


BASE = os.path.dirname(os.path.abspath(__file__))

@app.route('/chart.umd.min.js')
def chart_js():
    return send_from_directory(BASE, 'chart.umd.min.js')

@app.route('/')
def index():
    if not os.path.exists(mpd.OUT_FILE):
        return '<html><body style="background:#0f1117;color:#eee;font-family:sans-serif;padding:40px">' \
               '<h2>Dashboard not yet generated. Please wait a moment and refresh.</h2></body></html>'
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


if __name__ == '__main__':
    print('=' * 55)
    print(f'  Missed Pax Dashboard  —  Live Server')
    print('=' * 55)
    print(f'  Fetching initial data from DB ...')
    try:
        _do_refresh()
        print(f'  Done.  Opening http://localhost:{PORT}')
    except Exception as e:
        print(f'  WARNING: Initial fetch failed: {e}')
        print(f'  Serving whatever HTML exists. Use Refresh button to retry.')

    is_local = PORT == 8050 and not os.environ.get('RENDER') and not os.environ.get('RAILWAY_ENVIRONMENT')
    if is_local:
        threading.Timer(1.5, lambda: webbrowser.open(f'http://localhost:{PORT}')).start()
    print(f'  Press Ctrl+C to stop the server.\n')
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False, threaded=True)
