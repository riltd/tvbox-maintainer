#!/bin/sh
# TVBox maintainer wrapper: `update` = daily collect/validate/publish run,
# `serve-check` = make sure the static server on :9977 is up.
BASE=/vol1/1000/tvbox-maintainer
cd "$BASE" || exit 1

case "$1" in
  update)
    /usr/bin/python3 "$BASE/tvbox_maintain.py" >> "$BASE/run.log" 2>&1
    ;;
  serve-check)
    if ! ss -ltn | grep -q ':9977 '; then
      nohup /usr/bin/python3 "$BASE/serve.py" >> "$BASE/serve.log" 2>&1 &
    fi
    ;;
esac
