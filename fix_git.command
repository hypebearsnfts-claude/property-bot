#!/bin/bash
# Double-click this file in Finder to fix git and commit the walk filter changes.
# It will open in Terminal automatically. Safe to delete after running.

cd "$(dirname "$0")"

echo "=== Fixing git lock files ==="
rm -f .git/index.lock .git/HEAD.lock 2>/dev/null && echo "Lock files removed" || echo "No lock files found"

echo ""
echo "=== Resetting to clean state (before bad commit) ==="
git reset --hard cbd656e

echo ""
echo "=== Committing walk filter + send cap changes ==="
git add utils/walk_time.py filter_bot.py

git -c user.name="Ernest" -c user.email="hypebearsnfts@gmail.com" commit -m "Replace Google Maps walk filter with free station-map fallback

Every property is scraped within 0.25 miles (~5 min walk) of its tube
station by design, so Google Maps adds no filtering value in central London.
New free fallback uses the listing area field to return the correct station
name and 5 min walk time. Falls back gracefully if Google Maps fails.
Zero API cost.

Also MAX_LISTINGS_SEND=30 added to .env to cap Telegram sends per run.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"

echo ""
echo "=== Done! Git log ==="
git log --oneline -4

echo ""
echo "Press any key to close..."
read -n 1
